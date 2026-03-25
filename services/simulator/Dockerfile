FROM python:3.11-slim

WORKDIR /app

# Copy SDK and install it
COPY sdk ./sdk
RUN pip install --no-cache-dir -e ./sdk

# Copy requirements and install dependencies
COPY services/simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY services/simulator/ .

CMD ["python", "main.py"]
