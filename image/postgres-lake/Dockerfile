# PostgreSQL principal : image Spilo (opérateur Zalando, Patroni) + extension pg_lake
# (tables Iceberg lues et écrites depuis PostgreSQL, via le catalogue Polaris).
# La même image sert au sidecar pgduck_server (moteur DuckDB que pg_lake joint par socket Unix).
#   docker build -t postgres-lake:18 image/postgres-lake
# spilo-18 4.1-p2 (Ubuntu 22.04) : PostgreSQL 18 + 14 à 17 ; image par défaut de l'opérateur v2.0.1
ARG SPILO_IMAGE=ghcr.io/zalando/spilo-18:4.1-p2
FROM ${SPILO_IMAGE}

# version de pg_lake (paquet Debian du dépôt Pigsty) ; bibliothèques et pgduck_server doivent être de la même version
ARG PG_LAKE_VERSION=3.6*

# reprises : un dépôt lent ou un DNS capricieux ne fait pas échouer la construction
RUN set -eux; \
    echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-reprises; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL https://repo.pigsty.io/key | gpg --dearmor -o /etc/apt/keyrings/pigsty.gpg; \
    codename="$(. /etc/os-release && echo "$VERSION_CODENAME")"; \
    echo "deb [signed-by=/etc/apt/keyrings/pigsty.gpg] https://repo.pigsty.io/apt/pgsql/${codename} ${codename} main" \
      > /etc/apt/sources.list.d/pigsty.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends "postgresql-18-pg-lake=${PG_LAKE_VERSION}"; \
    rm -rf /var/lib/apt/lists/*; \
    test -x /usr/lib/postgresql/18/bin/pgduck_server; \
    ls /usr/share/postgresql/18/extension/ | grep -E '^pg_lake.*\.control$'
