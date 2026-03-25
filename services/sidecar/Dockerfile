FROM python:3.11-slim

WORKDIR /app

# Copy SDK and install it (build context is streamlineage/)
COPY sdk/ ./sdk/
RUN pip install --no-cache-dir -e ./sdk

COPY services/sidecar/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/sidecar/main.py .

CMD ["python", "main.py"]
