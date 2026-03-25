FROM python:3.11-slim

WORKDIR /app

# Copy SDK and install it (build context is streamlineage/)
COPY sdk/ ./sdk/
RUN pip install --no-cache-dir -e ./sdk

# Copy requirements and install dependencies
COPY services/lineage-ingester/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code (also copies lineage-ingester so relative imports work)
COPY services/lineage-ingester/ ./services/lineage-ingester/

WORKDIR /app/services/lineage-ingester

# Expose Prometheus metrics port
EXPOSE 9091

CMD ["python", "main.py"]
