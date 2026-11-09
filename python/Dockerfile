# Image du traitement planifié (pipeline.py) : client Spark Connect + code lakehouse
#   docker build -t lakehouse-pipeline:1.0 python
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyspark-client==4.1.3
COPY lakehouse/ lakehouse/
COPY pipeline.py .
USER 1000
ENTRYPOINT ["python", "pipeline.py"]
