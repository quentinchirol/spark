# Passerelle d'écriture de référence (FastAPI → Kafka), contexte : python/
#   docker build -t lakehouse-passerelle:1.0 -f python/passerelle/Dockerfile python
FROM python:3.12-slim
WORKDIR /app
COPY passerelle/requirements.txt passerelle/requirements.txt
RUN pip install --no-cache-dir -r passerelle/requirements.txt
COPY lakehouse/__init__.py lakehouse/contrat.py lakehouse/
COPY passerelle/ passerelle/
USER 1000
EXPOSE 8000
# plusieurs processus par pod : la validation JSON occupe le CPU, un seul processus Python plafonnerait
ENTRYPOINT ["uvicorn", "passerelle.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--no-access-log"]
