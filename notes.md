1. Create `clickstream` topic:

```
docker exec -it broker kafka-topics.sh \
  --create \
  --topic clickstream \
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

docker exec -it sql-gateway \
  /opt/flink/bin/sql-client.sh gateway -e "http://sql-gateway:8083"

1. Open Zeppelin UI at http://localhost:8082. 

1. Configure Flink interpreter settings:
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
%flinksql
SELECT * FROM sanity_test_table;
```

