FROM python:3.11-slim

WORKDIR /app

# Copy SDK and install it (build context is streamlineage/)
COPY sdk/ ./sdk/
RUN pip install --no-cache-dir -e ./sdk

# Copy requirements and install dependencies
COPY services/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy lineage-ingester source (API imports graph_builder, storage, schema_detector from it)
COPY services/lineage-ingester/ ./services/lineage-ingester/

# Copy API service code
COPY services/api/ ./services/api/

WORKDIR /app/services/api

# Expose API port
EXPOSE 8000

# Expose Prometheus metrics port
EXPOSE 9092

CMD ["python", "main.py"]
