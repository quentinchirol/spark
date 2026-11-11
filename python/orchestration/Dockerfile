# Dagster (serveur de code, interface web, démon) + orchestration des traitements, contexte : python/
#   docker build -t lakehouse-dagster:1.0 -f python/orchestration/Dockerfile python
FROM python:3.12-slim
ENV DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster/app
COPY orchestration/requirements.txt orchestration/requirements.txt
RUN pip install --no-cache-dir -r orchestration/requirements.txt \
 && mkdir -p "$DAGSTER_HOME" && chown 1000:1000 "$DAGSTER_HOME"
COPY orchestration/ orchestration/
USER 1000
