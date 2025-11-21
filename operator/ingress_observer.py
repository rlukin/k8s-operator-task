"""
Kubernetes Ingress Observer Operator

Watches Ingress resources and periodically reports summarized data
about them to a local HTTP endpoint.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import kopf
import kubernetes.client
import kubernetes.config
import requests

# Configuration from environment variables
CLUSTER_NAME = os.getenv("CLUSTER_NAME", "local-minikube")
REPORT_ENDPOINT = os.getenv("REPORT_ENDPOINT", "http://localhost:8080/report")
REPORT_INTERVAL = int(os.getenv("REPORT_INTERVAL", "45"))  # seconds

# Initialize Kubernetes API client
try:
    kubernetes.config.load_incluster_config()
except kubernetes.config.ConfigException:
    kubernetes.config.load_kube_config()

core_v1_api = kubernetes.client.CoreV1Api()
networking_v1_api = kubernetes.client.NetworkingV1Api()

# Configure logging level to INFO by default
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)




@kopf.on.create('ingresses')
def ingress_created(name: str, namespace: str, logger, **_):
    """Handle Ingress resource creation."""
    logger.info(f"Ingress created: {namespace}/{name}")


@kopf.on.update('ingresses')
def ingress_updated(name: str, namespace: str, logger, **_):
    """Handle Ingress resource updates."""
    logger.info(f"Ingress updated: {namespace}/{name}")


@kopf.on.delete('ingresses')
def ingress_deleted(name: str, namespace: str, logger, **_):
    """Handle Ingress resource deletion."""
    logger.info(f"Ingress deleted: {namespace}/{name}")


def extract_hosts(ingress_body: Dict) -> List[str]:
    """
    Extract hostnames from Ingress spec.rules.
    Returns a list of unique hostnames.
    """
    hosts = []
    spec = ingress_body.get('spec', {})
    rules = spec.get('rules', [])
    
    for rule in rules:
        host = rule.get('host')
        if host:
            hosts.append(host)
    
    return hosts


def get_tls_secret_name(ingress_body: Dict) -> Optional[str]:
    """
    Extract TLS secret name from Ingress spec.tls.
    Returns the first secret name found, or None.
    """
    spec = ingress_body.get('spec', {})
    tls = spec.get('tls', [])
    
    if tls and len(tls) > 0:
        secret_name = tls[0].get('secretName')
        return secret_name
    
    return None


def get_certificate_info(namespace: str, secret_name: Optional[str], logger) -> Optional[Dict]:
    """
    Read TLS Secret and extract certificate information.
    Returns a dict with certificate name and dummy expiry date.
    """
    if not secret_name:
        return None
    
    try:
        secret = core_v1_api.read_namespaced_secret(secret_name, namespace)
        
        # Check if secret has TLS data
        if secret.data and ('tls.crt' in secret.data or 'ca.crt' in secret.data):
            # Generate dummy expiry date (90 days from now)
            # In production, this would parse the x509 certificate
            expires = (datetime.utcnow() + timedelta(days=90)).isoformat() + "Z"
            
            return {
                "name": secret_name,
                "expires": expires
            }
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            logger.debug(f"Secret {namespace}/{secret_name} not found")
        else:
            logger.warning(f"Error reading secret {namespace}/{secret_name}: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error reading secret {namespace}/{secret_name}: {e}")
    
    return None


def build_report(ingresses_index: kopf.Index, logger) -> Dict:
    """
    Build the JSON report from indexed Ingress resources.
    Note: Kopf indexes store values as lists, so we need to extract the first element.
    """
    ingresses_list = []
    
    logger.info(f"build_report: index type={type(ingresses_index)}, length={len(ingresses_index) if hasattr(ingresses_index, '__len__') else 'N/A'}")
    
    if not ingresses_index:
        logger.info("build_report: index is empty")
        return {
            "cluster": CLUSTER_NAME,
            "ingresses": ingresses_list
        }
    
    # Iterate over all indexed Ingress resources
    # Kopf indexes store values as lists: {(namespace, name): [body]}
    for key, ingress_bodies in ingresses_index.items():
        try:
            namespace, name = key
            logger.debug(f"build_report: processing {namespace}/{name}, bodies type={type(ingress_bodies)}, is_list={isinstance(ingress_bodies, list)}")
            
            # Extract the first (and only) body from the list
            if not ingress_bodies:
                logger.debug(f"build_report: {namespace}/{name} has empty bodies list")
                continue
            
            if isinstance(ingress_bodies, list):
                if len(ingress_bodies) == 0:
                    logger.debug(f"build_report: {namespace}/{name} has empty list")
                    continue
                ingress_body = ingress_bodies[0]
            else:
                ingress_body = ingress_bodies
            
            logger.debug(f"build_report: extracted body for {namespace}/{name}, type={type(ingress_body)}")
            
            # Convert Store object to dict if needed
            # Kopf Store objects wrap the body - need to extract it
            if not isinstance(ingress_body, dict):
                logger.info(f"build_report: Converting Store for {namespace}/{name}, type={type(ingress_body).__name__}")
                
                # Store is a container - try to extract the actual body
                # Store has length 1, so it likely contains one body object
                try:
                    # Try to iterate and get first item
                    if hasattr(ingress_body, '__iter__'):
                        items = list(ingress_body)
                        if items and len(items) > 0:
                            ingress_body = items[0]
                            logger.info(f"build_report: Extracted body from Store (got {len(items)} items) for {namespace}/{name}")
                        else:
                            logger.error(f"build_report: Store is empty for {namespace}/{name}")
                            continue
                    else:
                        # Try direct attribute access
                        ingress_body = getattr(ingress_body, 'body', None) or getattr(ingress_body, 'data', None) or getattr(ingress_body, '_data', None)
                        if ingress_body is None:
                            logger.error(f"build_report: Cannot extract body from Store for {namespace}/{name}")
                            continue
                except Exception as e:
                    logger.error(f"build_report: Error extracting from Store for {namespace}/{name}: {e}", exc_info=True)
                    continue
                
                # Now try to convert to dict if still not a dict
                if not isinstance(ingress_body, dict):
                    try:
                        ingress_body = dict(ingress_body)
                        logger.info(f"build_report: Converted to dict for {namespace}/{name}")
                    except (TypeError, ValueError):
                        # If it's already dict-like (has get or __getitem__), use it as is
                        if hasattr(ingress_body, 'get') or hasattr(ingress_body, '__getitem__'):
                            logger.info(f"build_report: Using dict-like object for {namespace}/{name}")
                        else:
                            logger.error(f"build_report: Cannot convert to dict for {namespace}/{name}, type={type(ingress_body)}")
                            continue
            
            # Final check
            if not isinstance(ingress_body, dict) and not (hasattr(ingress_body, 'get') or hasattr(ingress_body, '__getitem__')):
                logger.error(f"build_report: {namespace}/{name} body is not dict-like, type={type(ingress_body)}")
                continue
            
            hosts = extract_hosts(ingress_body)
            logger.debug(f"build_report: {namespace}/{name} has hosts: {hosts}")
            
            # Skip if no hosts found
            if not hosts:
                logger.debug(f"build_report: {namespace}/{name} skipped - no hosts")
                continue
            
            # Get TLS secret name
            tls_secret_name = get_tls_secret_name(ingress_body)
            
            # Get certificate info
            certificate_info = None
            if tls_secret_name:
                certificate_info = get_certificate_info(namespace, tls_secret_name, logger)
            
            # Create entry for each host (or one entry with all hosts)
            # Per the example, we'll create one entry per host
            for host in hosts:
                ingress_entry = {
                    "namespace": namespace,
                    "name": name,
                    "host": host
                }
                
                if certificate_info:
                    ingress_entry["certificate"] = certificate_info
                
                ingresses_list.append(ingress_entry)
        
        except Exception as e:
            logger.error(f"Error processing Ingress {key}: {e}", exc_info=True)
            continue
    
    return {
        "cluster": CLUSTER_NAME,
        "ingresses": ingresses_list
    }


def send_report(report: Dict, logger_param=None) -> bool:
    """
    Send the report to the HTTP endpoint via POST.
    Returns True if successful, False otherwise.
    
    Note: Always uses standard Python logger since this function runs
    in a thread pool executor where Kopf's logger context is not available.
    """
    # Always use standard Python logger to avoid ContextVar errors in threads
    try:
        response = requests.post(
            REPORT_ENDPOINT,
            json=report,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        logger.info(f"Report sent successfully: {len(report['ingresses'])} ingresses")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send report: {e}")
        return False


# Global reference to store the index for periodic reporting
_ingresses_index_storage = {}
_last_report_time = 0


@kopf.index('ingresses')
def index_ingresses(name: str, namespace: str, body: kopf.Body, **_):
    """
    Index all Ingress resources by (namespace, name) tuple.
    """
    return {(namespace, name): body}


@kopf.on.startup()
async def startup_handler(logger, settings: kopf.OperatorSettings = None, **_):
    """
    Handler called when the operator starts.
    Creates a background task for periodic reporting.
    """
    import asyncio
    
    # Configure logging level to INFO
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger(__name__).setLevel(logging.INFO)
    
    # Configure Kopf settings if available
    if settings:
        settings.posting.level = logging.INFO
    
    logger.info(f"Ingress Observer Operator starting")
    logger.info(f"Cluster: {CLUSTER_NAME}")
    logger.info(f"Report endpoint: {REPORT_ENDPOINT}")
    logger.info(f"Report interval: {REPORT_INTERVAL} seconds")
    
    # Create background task for periodic reporting
    asyncio.create_task(periodic_report_task(logger))


@kopf.on.probe(id='periodic-report')
async def periodic_report_probe(index_ingresses: kopf.Index, logger, **_):
    """
    Probe handler that can access the index.
    This is called periodically and updates the global storage.
    """
    # Update global storage with current index
    _ingresses_index_storage['index'] = index_ingresses
    logger.info(f"Probe handler: index contains {len(index_ingresses)} entries")
    return {"index_size": len(index_ingresses)}


@kopf.timer('ingresses', interval=REPORT_INTERVAL, initial_delay=REPORT_INTERVAL)
async def periodic_report_handler(index_ingresses: kopf.Index, logger, name, namespace, **_):
    """
    Timer handler that runs periodically on any Ingress resource.
    Updates the global index storage and sends reports.
    This ensures we have proper access to the index via injection.
    Uses a timestamp check to ensure only one report per interval.
    """
    import time
    
    global _last_report_time
    
    # Update global storage with current index
    _ingresses_index_storage['index'] = index_ingresses
    
    logger.info(f"Timer handler fired for {namespace}/{name}, index contains {len(index_ingresses)} entries")
    
    # Log some index details for debugging
    if index_ingresses:
        sample_keys = list(index_ingresses.keys())[:3]
        logger.info(f"Sample index keys: {sample_keys}")
        # Log the actual structure
        for key, value in list(index_ingresses.items())[:2]:
            logger.info(f"Index entry: key={key}, value_type={type(value)}, value_len={len(value) if hasattr(value, '__len__') else 'N/A'}")
    
    # Only send report if enough time has passed (prevent duplicates)
    current_time = time.time()
    if current_time - _last_report_time < REPORT_INTERVAL - 1:
        logger.debug(f"Skipping report - too soon since last report ({current_time - _last_report_time:.1f}s ago)")
        return
    
    _last_report_time = current_time
    
    # Build and send report
    logger.info("Building periodic report from timer handler...")
    report = build_report(index_ingresses, logger)
    logger.info(f"Report contains {len(report['ingresses'])} ingress entries")
    
    # Send report (non-blocking via executor)
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, send_report, report, logger)


async def periodic_report_task(logger):
    """
    Background task that periodically builds and sends reports.
    This runs independently of Ingress resources, ensuring reports
    are sent even when there are no ingresses.
    Accesses the index directly from Kopf's registry.
    """
    import asyncio
    
    # Wait initial delay before first report
    await asyncio.sleep(REPORT_INTERVAL)
    
    while True:
        try:
            logger.info("Building periodic report...")
            
            # Try to get the index from multiple sources
            ingresses_index = {}
            
            # First, try global storage (updated by timer handler)
            if 'index' in _ingresses_index_storage:
                ingresses_index = _ingresses_index_storage['index']
                logger.info(f"Using index from global storage: {len(ingresses_index)} entries")
            else:
                # Fallback: try to access registry directly
                try:
                    # Access the registry indexes - the index name matches the function name
                    if hasattr(kopf, 'registry') and hasattr(kopf.registry, 'indexes'):
                        registry_indexes = kopf.registry.indexes
                        if hasattr(registry_indexes, 'get'):
                            ingresses_index = registry_indexes.get('index_ingresses', {})
                            logger.info(f"Using index from registry.get(): {len(ingresses_index)} entries")
                        elif 'index_ingresses' in registry_indexes:
                            ingresses_index = registry_indexes['index_ingresses']
                            logger.info(f"Using index from registry dict access: {len(ingresses_index)} entries")
                        else:
                            logger.warning(f"Registry indexes keys: {list(getattr(registry_indexes, 'keys', lambda: [])())}")
                    else:
                        logger.warning("Registry or indexes not accessible")
                except Exception as reg_error:
                    logger.warning(f"Could not access registry: {reg_error}", exc_info=True)
                    ingresses_index = {}
            
            logger.info(f"Background task: index contains {len(ingresses_index)} entries")
            
            # Log index contents for debugging
            if ingresses_index:
                keys_list = list(ingresses_index.keys())[:5]
                logger.info(f"Index keys (first 5): {keys_list}")
            
            report = build_report(ingresses_index, logger)
            
            logger.info(f"Report contains {len(report['ingresses'])} ingress entries")
            
            # Run the blocking HTTP request in a thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, send_report, report, logger)
            
            await asyncio.sleep(REPORT_INTERVAL)
        
        except asyncio.CancelledError:
            logger.info("Periodic report task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in periodic report task: {e}", exc_info=True)
            await asyncio.sleep(REPORT_INTERVAL)

