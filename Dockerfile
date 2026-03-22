FROM apache/flink:1.17.2-scala_2.12

USER root

# ---------------- SYSTEM SETUP ----------------
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python

# ---------------- PIP CONFIG (FIX TIMEOUT ISSUES) ----------------
RUN pip3 config set global.index-url https://pypi.org/simple
RUN pip3 config set global.timeout 100

# ---------------- SET WORKDIR ----------------
WORKDIR /opt/flink/usrlib

# ---------------- COPY PROJECT FILES ----------------
COPY . .

# ---------------- PYTHON DEPENDENCIES ----------------
RUN pip3 install --default-timeout=100 --retries=5 \
    apache-flink==1.17.2 \
    kafka-python \
    psycopg2-binary \
    numpy \
    protobuf

# Install from requirements.txt
RUN pip3 install --default-timeout=100 --retries=5 \
    --no-cache-dir -r requirements.txt

# Ensure streamlit (critical for dashboard)
RUN pip3 install --default-timeout=100 --retries=5 streamlit

# ---------------- KAFKA CLIENT ----------------
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -P /opt/flink/lib/

# ---------------- FLINK KAFKA CONNECTOR ----------------
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar \
    -P /opt/flink/lib/

    # 🔥 Enable Prometheus metrics
RUN echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> /opt/flink/conf/flink-conf.yaml && \
    echo "metrics.reporter.prom.port: 9249" >> /opt/flink/conf/flink-conf.yaml && \
    echo "metrics.reporter.prom.host: 0.0.0.0" >> /opt/flink/conf/flink-conf.yaml
    
USER flink