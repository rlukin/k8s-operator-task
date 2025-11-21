"""
Report Receiver Service

Simple HTTP service that receives reports from the ingress observer operator
and displays them in a web UI.
"""
import os
import json
from datetime import datetime
from collections import deque
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

# Configuration
MAX_REPORTS = int(os.getenv("MAX_REPORTS", "100"))
PORT = int(os.getenv("PORT", "8080"))

# In-memory storage for reports (FIFO queue)
reports = deque(maxlen=MAX_REPORTS)


@app.route('/report', methods=['POST'])
def receive_report():
    """Receive a report from the operator."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        # Add timestamp to the report
        report_with_timestamp = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "report": data
        }
        
        reports.append(report_with_timestamp)
        
        return jsonify({"status": "received", "report_count": len(reports)}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/', methods=['GET'])
def display_reports():
    """Display all received reports in a simple HTML UI."""
    reports_list = list(reports)
    
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Ingress Observer Reports</title>
    <style>
        body {
            font-family: monospace;
            margin: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
        }
        .report {
            background-color: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 15px;
            margin: 10px 0;
        }
        .timestamp {
            color: #666;
            font-size: 0.9em;
            margin-bottom: 10px;
        }
        .cluster {
            font-weight: bold;
            color: #2196F3;
            margin-bottom: 10px;
        }
        .ingress {
            margin-left: 20px;
            margin-top: 5px;
            padding-left: 10px;
            border-left: 2px solid #ddd;
        }
        .host {
            color: #4CAF50;
        }
        .certificate {
            color: #FF9800;
            margin-left: 10px;
        }
        .no-reports {
            color: #999;
            font-style: italic;
        }
        pre {
            background-color: #f9f9f9;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
        }
    </style>
</head>
<body>
    <h1>Ingress Observer Reports</h1>
    <p>Total reports received: {{ report_count }}</p>
    
    {% if reports %}
        {% for item in reports %}
        <div class="report">
            <div class="timestamp">Received: {{ item.timestamp }}</div>
            <div class="cluster">Cluster: {{ item.report.cluster }}</div>
            <div>Ingresses: {{ item.report.ingresses|length }}</div>
            
            {% if item.report.ingresses %}
            <div style="margin-top: 10px;">
                {% for ingress in item.report.ingresses %}
                <div class="ingress">
                    <span class="host">{{ ingress.host }}</span>
                    <span style="color: #666;">({{ ingress.namespace }}/{{ ingress.name }})</span>
                    {% if ingress.certificate %}
                    <div class="certificate">
                        Certificate: {{ ingress.certificate.name }}
                        {% if ingress.certificate.expires %}
                        <br>Expires: {{ ingress.certificate.expires }}
                        {% endif %}
                    </div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
            {% endif %}
            
            <details style="margin-top: 10px;">
                <summary style="cursor: pointer; color: #666;">View Raw JSON</summary>
                <pre>{{ item.report|tojson(indent=2) }}</pre>
            </details>
        </div>
        {% endfor %}
    {% else %}
        <div class="no-reports">No reports received yet.</div>
    {% endif %}
    
    <p style="margin-top: 20px; color: #666; font-size: 0.9em;">
        Last updated: {{ current_time }}
    </p>
</body>
</html>
    """
    
    return render_template_string(
        html_template,
        reports=reports_list,
        report_count=len(reports_list),
        current_time=datetime.utcnow().isoformat() + "Z"
    )


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "report_count": len(reports)}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)

