FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy operator code
COPY operator/ ./operator/

# Run the operator
CMD ["kopf", "run", "operator/ingress_observer.py", "--verbose"]

