FROM astrocrpublic.azurecr.io/runtime:3.2-3

USER root

RUN apt-get update && \
    apt-get install -y default-jre-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.5/gcs-connector-hadoop3-2.2.5-shaded.jar

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER astro