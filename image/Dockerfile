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

# Connecteur Kafka de Spark (lecture du topic « events », k8s/traitements/evenements.py) et ses dépendances,
# résolus par Maven avec vérification stricte des sommes de contrôle (image/kafka/pom.xml)
FROM maven:3.9.11-eclipse-temurin-21 AS kafka
COPY kafka/pom.xml /build/pom.xml
RUN mvn -B -q -C -f /build/pom.xml dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/build/kafka

FROM apache/spark:4.1.3-python3
COPY --from=build /build/jars/ /opt/spark/jars/
COPY --from=kafka /build/kafka/ /tmp/kafka/
# n'ajouter que les jars absents de Spark : un artefact déjà fourni par Spark garde sa version
USER root
RUN for jar in /tmp/kafka/*.jar; do \
      nom=$(basename "$jar" .jar); nom=${nom%%-[0-9]*}; \
      if ls /opt/spark/jars/"$nom"-[0-9]*.jar >/dev/null 2>&1; then echo "déjà dans Spark : $(basename "$jar")"; \
      else echo "ajouté : $(basename "$jar")"; install -m 644 "$jar" /opt/spark/jars/; fi; \
    done \
 && rm -rf /tmp/kafka \
 && ls /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/kafka-clients-*.jar

# traitements à la demande (k8s/traitements) : pyarrow lit les fichiers sources (CSV) dans MinIO ; kafka-python lit
# les offsets du topic « events » et y enregistre la position atteinte (suivi du retard)
RUN python3 -m pip install --no-cache-dir pyarrow==21.0.0 kafka-python==3.0.13 \
 || python3 -m pip install --no-cache-dir --break-system-packages pyarrow==21.0.0 kafka-python==3.0.13
USER spark
