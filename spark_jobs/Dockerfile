FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install dependencies for spark
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /app
WORKDIR /app

USER jovyan

CMD ["python", "bronze_ingestion.py"]
