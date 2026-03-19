FROM apache/flink:1.17.2-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.17.2 kafka-python psycopg2-binary numpy protobuf

# Kafka client
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -P /opt/flink/lib/

# 🔥 Flink Kafka connector (THIS WAS MISSING)
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar \
    -P /opt/flink/lib/

USER flink