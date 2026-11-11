# Image du writer Kafka → PostgreSQL (python -m ecrivain), contexte : python/
#   docker build -t lakehouse-ecrivain:1.0 -f python/ecrivain/Dockerfile python
FROM python:3.12-slim
WORKDIR /app
COPY ecrivain/requirements.txt ecrivain/requirements.txt
RUN pip install --no-cache-dir -r ecrivain/requirements.txt
COPY lakehouse/__init__.py lakehouse/contrat.py lakehouse/
COPY ecrivain/ ecrivain/
USER 1000
ENTRYPOINT ["python", "-m", "ecrivain"]
