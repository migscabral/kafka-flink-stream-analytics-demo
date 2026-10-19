docker build --platform linux/amd64 -t zeppelin-flink:0.11.2-1.17.2 -f "zeppelin-flink:0.11.2-1.17.2.Dockerfile" .

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
    'properties.bootstrap.servers' = 'broker:29092',
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

## Iceberg on S3 via AWS Glue

1. Fill in the `.env` file in the repo root. `docker-compose` reads it automatically when run from this directory, and the values are passed to the Flink and Zeppelin containers:
```
AWS_REGION=us-east-1
S3_BUCKET=<your bucket name, without s3://>
AWS_ACCESS_KEY_ID=<your access key id>
AWS_SECRET_ACCESS_KEY=<your secret access key>
```
The identity needs Glue and S3 read/write permissions on the warehouse bucket. Attach [aws/iceberg-flink-policy.json](aws/iceberg-flink-policy.json) to the IAM user after replacing `REGION`, `ACCOUNT_ID` and `YOUR_BUCKET`. `.env` is git-ignored; never commit it. Variables already exported in your shell take precedence over `.env`.

1. Recreate the stack so the Iceberg jars and credentials are picked up (re-run this after any `.env` change): `docker-compose --profile core up -d --force-recreate`

1. In Zeppelin, run the "Iceberg sink" paragraphs at the end of `clickstream-analysis.flink.sql`. The catalog paragraph builds the warehouse path `s3://$S3_BUCKET/warehouse` from the `S3_BUCKET` variable in `.env`.

## Fraudulent transactions topic

1. Create the topic (optional; the broker also auto-creates it on first write):
```
docker exec -it broker kafka-topics.sh \
  --create \
  --topic fraudulent-transactions \
  --bootstrap-server broker:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=864000000
```

1. In Zeppelin, run the "Fraudulent Transactions Stream" paragraphs in `clickstream-analysis.flink.sql`. The `INSERT INTO` starts a long-running job that writes one JSON record per fraudulent flow (view, scroll, add to cart and purchase completed in 1 second or less).

1. Read the output:
```
docker exec -it broker kafka-console-consumer.sh \
  --topic fraudulent-transactions \
  --bootstrap-server broker:9092 \
  --from-beginning
```
