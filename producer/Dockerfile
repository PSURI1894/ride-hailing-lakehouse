FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (build-essential needed for confluent-kafka in some cases)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ schemas/
COPY src/ src/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "src/main.py"]
