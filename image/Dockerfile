# Image unique du serveur Spark Connect et des executors :
# Spark 4.1.3 (version testée par Iceberg 1.12.0) + Iceberg + extension auth/audit
FROM apache/spark:4.1.3-python3 AS build
USER root

COPY src /build/src
RUN mkdir /build/classes /build/jars \
 && javac --release 21 -proc:none -cp "/opt/spark/jars/*" -d /build/classes $(find /build/src -name '*.java') \
 && jar cf /build/jars/spark-connect-auth.jar -C /build/classes .

# Iceberg pour Spark 4.1 + client S3 (S3FileIO), vérifiés par empreinte SHA-1 Maven Central
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /build/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /build/jars/
RUN cd /build/jars \
 && echo "9e395e373a9a99643bf413ca1f74fa0c034887b4  iceberg-spark-runtime-4.1_2.13-1.12.0.jar" | sha1sum -c \
 && echo "64d5012217035a60924feb633636b744d318eb0a  iceberg-aws-bundle-1.12.0.jar" | sha1sum -c \
 && chmod 644 *.jar

FROM apache/spark:4.1.3-python3
COPY --from=build /build/jars/ /opt/spark/jars/

# pyarrow : lecture des fichiers sources (CSV) dans MinIO par les traitements à la demande (python/traitements)
USER root
RUN python3 -m pip install --no-cache-dir pyarrow==21.0.0 \
 || python3 -m pip install --no-cache-dir --break-system-packages pyarrow==21.0.0
USER spark
