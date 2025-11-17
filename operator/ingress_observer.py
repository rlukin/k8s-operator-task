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

logger = logging.getLogger(__name__)




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
    """
    ingresses_list = []
    
    # Iterate over all indexed Ingress resources
    for (namespace, name), ingress_body in ingresses_index.items():
        try:
            hosts = extract_hosts(ingress_body)
            
            # Skip if no hosts found
            if not hosts:
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
            logger.error(f"Error processing Ingress {namespace}/{name}: {e}")
            continue
    
    return {
        "cluster": CLUSTER_NAME,
        "ingresses": ingresses_list
    }


def send_report(report: Dict, logger) -> bool:
    """
    Send the report to the HTTP endpoint via POST.
    Returns True if successful, False otherwise.
    """
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


@kopf.index('ingresses')
def index_ingresses(name: str, namespace: str, body: kopf.Body, ingresses_index: kopf.Index, **_):
    """
    Index all Ingress resources by (namespace, name) tuple.
    Also stores the index reference for periodic reporting.
    """
    # Store the index reference globally for the background task
    _ingresses_index_storage['index'] = ingresses_index
    return {(namespace, name): body}


@kopf.on.startup()
async def startup_handler(logger, **_):
    """
    Handler called when the operator starts.
    Creates a background task for periodic reporting.
    """
    import asyncio
    
    logger.info(f"Ingress Observer Operator starting")
    logger.info(f"Cluster: {CLUSTER_NAME}")
    logger.info(f"Report endpoint: {REPORT_ENDPOINT}")
    logger.info(f"Report interval: {REPORT_INTERVAL} seconds")
    
    # Create background task for periodic reporting
    asyncio.create_task(periodic_report_task(logger))


async def periodic_report_task(logger):
    """
    Background task that periodically builds and sends reports.
    This runs independently of Ingress resources, ensuring reports
    are sent even when there are no ingresses.
    """
    import asyncio
    
    # Wait initial delay before first report
    await asyncio.sleep(REPORT_INTERVAL)
    
    while True:
        try:
            logger.info("Building periodic report...")
            
            # Get the index from storage (may be empty if no ingresses yet)
            ingresses_index = _ingresses_index_storage.get('index', {})
            
            report = build_report(ingresses_index, logger)
            
            logger.debug(f"Report contains {len(report['ingresses'])} ingress entries")
            
            # Run the blocking HTTP request in a thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, send_report, report, logger)
            
            await asyncio.sleep(REPORT_INTERVAL)
        
        except asyncio.CancelledError:
            logger.info("Periodic report task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in periodic report task: {e}")
            await asyncio.sleep(REPORT_INTERVAL)

