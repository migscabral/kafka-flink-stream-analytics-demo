1. docker-compose --profile core up -d --force-recreate

1. Create `sanity-test` topic:

```
docker exec -it broker kafka-topics.sh \
  --create \
  --topic sanity-test \
  --bootstrap-server broker:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=864000000
```
1. Producer sanity check
```
docker exec -it broker kafka-console-producer.sh \
  --topic sanity-test \
  --bootstrap-server broker:9092
```

Once producer is running, input the following messages:
```
{"user_id":1,"action":"click"}
{"user_id":2,"action":"view"}
```

1. Consumer sanity check
```
docker exec -it broker kafka-console-consumer.sh \
  --topic sanity-test \
  --bootstrap-server broker:9092 \
  --from-beginning
```

1. Open Zeppelin UI at http://localhost:8082. 

1. Configure Flink interpreter settings, then save, and restart Flink interpreter.
```
FLINK HOME = /opt/flink
flink.execution.mode = remote
flink.execution.remote.host = jobmanager
flink.execution.remote.port = 8081
```

1. Create a notebook and select **flink** as the interpreter. Input the following code in a paragraph and run:
```
%flink.ssql
CREATE TABLE sanity_test_table (
    user_id INT,
    action STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sanity-test',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'sanity-test-group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);
```

1. Input the following code in another paragraph and run:
```
%flink.ssql
SELECT * FROM sanity_test_table;
```

docker exec -it broker kafka-topics.sh \
  --delete \
  --topic clickstream \
  --bootstrap-server broker:9092

```
docker exec -it broker kafka-topics.sh \
  --create \
  --topic clickstream \
  --bootstrap-server broker:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=864000000
```


docker build -t clickstream-generator:latest .

docker-compose --profile core up -d --force-recreate

docker-compose --profile datagen up -d --force-recreate