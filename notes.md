# OSS Stream Processing

Local demo: a generator writes clickstream events to Kafka, Flink (run from Zeppelin notebooks) analyzes them, and the results go to a Kafka topic and to Iceberg tables on S3 (AWS Glue catalog). DuckDB queries the Iceberg tables.

Run every command in this file from the repo root.

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Flink dashboard | http://localhost:8081 |
| Zeppelin | http://localhost:8082 |

## 1. Configure

1. Copy `.env.template` to `.env` and fill it in. `docker-compose` passes these values to the Flink and Zeppelin containers through `env_file`, so variables exported in your shell do not override them:
```
AWS_REGION=<region of the bucket>
S3_BUCKET=<bucket name, without s3://>
AWS_ACCESS_KEY_ID=<access key id>
AWS_SECRET_ACCESS_KEY=<secret access key>
```

2. Attach [aws/iceberg-flink-policy.json](aws/iceberg-flink-policy.json) to the IAM user after replacing `REGION`, `ACCOUNT_ID` and `YOUR_BUCKET`. `.env` is git-ignored; never commit it.

## 2. Build images

```
docker build --platform linux/amd64 -t zeppelin-flink:0.11.2-1.17.2 -f "zeppelin-flink:0.11.2-1.17.2.Dockerfile" .
docker build -t clickstream-generator:latest ./datagen
```

## 3. Start the stack

```
docker-compose --profile core up -d --force-recreate
docker-compose --profile core ps
```

Re-run the first command after changing `.env` or `docker-compose.yaml`. Recreating containers stops all running Flink jobs and resets the Zeppelin session, so re-run the notebook paragraphs afterwards.

To redeploy only a specific service (e.g. task manager):
```
docker-compose --profile core up -d --no-deps --force-recreate taskmanager
```

## 4. Sanity check (Kafka and Flink)

Flink tables use the broker address `broker:29092` (inside the Docker network). The `docker exec` commands below run inside the broker container, where `broker:9092` is correct.

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
2. Producer sanity check
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

3. Consumer sanity check
```
docker exec -it broker kafka-console-consumer.sh \
  --topic sanity-test \
  --bootstrap-server broker:9092 \
  --from-beginning
```

4. Open Zeppelin UI at http://localhost:8082.

5. Configure Flink interpreter settings, then save, and restart Flink interpreter.
```
FLINK HOME = /opt/flink
flink.execution.mode = remote
flink.execution.remote.host = jobmanager
flink.execution.remote.port = 8081
```

6. Create a notebook and select **flink** as the interpreter. Input the following code in a paragraph and run:
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

7. Input the following code in another paragraph and run:
```
%flink.ssql
SELECT * FROM sanity_test_table;
```

## 5. Clickstream pipeline

1. Reset the `clickstream` topic (optional, to start clean):
```
docker exec -it broker kafka-topics.sh \
  --delete \
  --topic clickstream \
  --bootstrap-server broker:9092

docker exec -it broker kafka-topics.sh \
  --create \
  --topic clickstream \
  --bootstrap-server broker:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=864000000
```

2. Start the generator:
```
docker-compose --profile datagen up -d --force-recreate
```

3. In Zeppelin, run the paragraphs in `clickstream-analysis.flink.sql` one at a time: the `clickstream` table first, then the analysis queries (completed flows, fraudulent flow counts, tumbling and session windows).

## 6. Iceberg on S3 (AWS Glue catalog)

1. In Zeppelin, run the paragraphs under "6. Iceberg on S3" in `clickstream-analysis.flink.sql`, in order: register the `glue_catalog`, then create the `clickstream_db` database and the `clickstream_events` table. The `INSERT INTO` jobs that fill the tables are at the end of the file and are started in section 7.

2. Check S3 once the jobs are running (section 7, step 3): data files appear under `s3://<bucket>/warehouse/clickstream_db.db/clickstream_events/` after the first checkpoint (every 10 seconds). Iceberg only commits on checkpoint.

Notes:
- The catalog paragraph uses the Scala `%flink` interpreter to read `S3_BUCKET` and builds `s3://$S3_BUCKET/warehouse`. `%flink.ssql` cannot read environment variables.
- Do not use `${...}` in Zeppelin paragraphs. Zeppelin treats it as a dynamic form and replaces it with an empty string.
- The catalog uses `catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog`. Iceberg 1.6.1 (the last version for Flink 1.17) does not accept `catalog-type = glue`.
- Credentials are the IAM user keys from `.env`. Do not use temporary or SSO credentials, which expire and break running jobs.

## 7. Fraudulent transactions

A flow is fraudulent when view, scroll, add to cart and purchase complete within 1 second.

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

2. Create the tables: in Zeppelin, run the paragraphs under "7. Fraudulent transactions" in `clickstream-analysis.flink.sql` (the Kafka sink table, the Kafka source table and the Iceberg table).

3. Start the streaming job: run the paragraphs under "Streaming jobs" at the end of the SQL file: the checkpoint setting, then the single `%flink.ssql(runAsOne=true)` paragraph. It submits the three `INSERT INTO` statements (clickstream to Iceberg, fraudulent flows to Kafka, Kafka to Iceberg) as one Flink job. The paragraph stays in the Running state until you cancel it. Separate `INSERT INTO` paragraphs did not run in parallel in Zeppelin: the second one stayed Pending.

4. Read the output:
```
docker exec -it broker kafka-console-consumer.sh \
  --topic fraudulent-transactions \
  --bootstrap-server broker:9092 \
  --from-beginning
```

## 8. Query the Iceberg tables with DuckDB

Tested with DuckDB 1.5.6. The tables are `glue_catalog.clickstream_db.clickstream_events` and `glue_catalog.clickstream_db.fraudulent_transactions`.

1. Attach the Glue catalog:
```sql
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE OR REPLACE SECRET (
    TYPE s3,
    KEY_ID '<access key id>',
    SECRET '<secret access key>',
    REGION '<region of the bucket>'
);

ATTACH '<ACCOUNT_ID>' AS glue_catalog (TYPE iceberg, ENDPOINT_TYPE glue);
```

2. Query:
```sql
SHOW ALL TABLES;
SELECT * FROM glue_catalog.clickstream_db.clickstream_events LIMIT 10;
SELECT * FROM glue_catalog.clickstream_db.fraudulent_transactions LIMIT 10;
```

`ATTACH` calls the Glue Iceberg REST endpoint, which needs `glue:GetCatalog` on the IAM user (included in the policy file). Do not save the keys in a file you commit.

If `ATTACH` does not work, read a table through its current metadata file instead. The path changes at every checkpoint, so look it up each time:
```bash
aws glue get-table --region <region> --database-name clickstream_db \
  --name clickstream_events --query 'Table.Parameters.metadata_location' --output text
```
```sql
SELECT * FROM iceberg_scan('<metadata_location from above>') LIMIT 10;
```

## 9. Clean start (destructive)

Use this to delete everything and start over. Run the steps in this order.

1. Cancel all running Flink jobs in the Flink dashboard (http://localhost:8081). Running `INSERT INTO` jobs keep writing to the tables you are about to drop.

2. Drop the Iceberg tables and the Glue database from Zeppelin: run the "Clean start" paragraphs in section 6 of `clickstream-analysis.flink.sql`. Flink 1.17 has no `DROP CATALOG`; the next paragraph unregisters `glue_catalog` from the session. Re-run the catalog paragraph afterwards to use Iceberg again.

3. Or delete them with the AWS CLI instead (the tables first, then the database, then the files in S3):
```
aws glue delete-table --region <region> --database-name clickstream_db --name clickstream_events
aws glue delete-table --region <region> --database-name clickstream_db --name fraudulent_transactions
aws glue delete-database --region <region> --name clickstream_db
aws s3 rm s3://<bucket>/warehouse/ --recursive
```
Check for leftovers with `aws s3 ls s3://<bucket>/warehouse/ --recursive`. The IAM policy in `aws/` allows these actions.

4. Delete the Kafka topics:
```
docker exec -it broker kafka-topics.sh --delete --topic clickstream --bootstrap-server broker:9092
docker exec -it broker kafka-topics.sh --delete --topic fraudulent-transactions --bootstrap-server broker:9092
docker exec -it broker kafka-topics.sh --delete --topic sanity-test --bootstrap-server broker:9092
```
Then create `clickstream` again as in section 5.

5. Restart the stack to reset the Flink and Zeppelin state (in-memory tables and catalogs are lost on recreate):
```
docker-compose --profile core --profile datagen down
docker-compose --profile core up -d
```

## 10. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `pull access denied` or platform errors from `docker-compose` | Run from the repo root, not from `datagen`. Build the `zeppelin-flink` image first. The amd64 warning on Apple Silicon is harmless: `apache/zeppelin` has no arm64 image. |
| Flink: `Timed out waiting for a node assignment` | Use `broker:29092` in Flink tables. Port 9092 advertises `localhost`. |
| `Unknown catalog-type: glue` | Use `catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog`. |
| `Bucket cannot be empty` on `CREATE TABLE` | The catalog was created with an empty bucket, usually from `${...}` in a Zeppelin paragraph. Build the path by string concatenation and restart the Flink interpreter. |
| `catalog already exists` in Zeppelin | Unregister it with the Scala paragraph in section 6 of the SQL file, or restart the Flink interpreter. |
| `security token ... expired` | A container got temporary credentials. Use the IAM user keys in `.env`, no session token, and recreate the containers. |
| Glue `AccessDenied` (for example `DeleteDatabase`, `GetCatalog`) | The IAM policy is missing an action or resource. Use the policy file in `aws/`. |
| DuckDB `ATTACH` returns 403 | The IAM user lacks `glue:GetCatalog` or S3 access to the bucket. Check the policy. |
| New Flink job stays waiting | No free task slot. The task manager has 8 (`taskmanager.numberOfTaskSlots`). |
