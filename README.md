# data-eng-proj
## Entire project workflow/documentation

## Day 1
1. Installed docker on local machine
2. Wrote docker-compose.yml file with configurations required for other envs
3. Kafka in kraft mode instead of zookeeper mode- lightweight. Was not familiar with this so had to debug a lot. Learnt that this required a clusterID. Has 2 ways, confluent and bitnami. 
4. Kafka up, created 3 topics


## Comands to reach till here
1. Clean old files
```
docker compose down -v
```

2. Restart all
```
docker compose up -d
```
3. Check if all containers have successfully loaded
```
docker ps
```
Should have 
1. flink jobmanager
2. flink taskmanager
3. postgres
4. confluent kafka

4. Enter kafka bash 
```
docker exec -it kafka bash
```

Create 3 topics:
```
kafka-topics --create --topic telemetry_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

```
kafka-topics --create --topic weather_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

```
kafka-topics --create --topic pitstop_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
Verify:
```
kafka-topics --list --bootstrap-server localhost:9092
```
5. Open a new terminal, for python producer
``` 
python -m venv venv
```

```
venv\Scripts\activate
```

6. Install kafka client if not done yet
```
pip install kafka-python
```

7. Run the producer
```
python telemetry_producer.py
```

8. Verify data at kafka consumer
```
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic telemetry_stream \
  --from-beginning
  ```
9. We have 3 producer files: telemetry, pitstop and weather.
To run producer:
```
python weather_producer.py
python pitstop_producer.py
```
To check consumer:
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_stream
kafka-console-consumer --bootstrap-server localhost:9092 --topic pitstop_stream
```
10. Kafka and python producer done, now flink time.
