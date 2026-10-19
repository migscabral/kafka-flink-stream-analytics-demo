-- 5. Clickstream pipeline
-- Run the clickstream table first, then the analysis queries below one at a time.

%flink.ssql
DROP TABLE IF EXISTS clickstream;

%flink.ssql
CREATE TABLE clickstream (
    user_id STRING,
    product_id STRING,
    action STRING,
    `timestamp` STRING,
    ts_ltz AS CAST(
        TO_TIMESTAMP(
            SUBSTRING(`timestamp`, 1, 26), 
            'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'
        )
        AS TIMESTAMP_LTZ(3) 
    ),
    WATERMARK FOR ts_ltz AS ts_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'clickstream_consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Total Completed Flows Per User (Core Metric)
-- This query uses MATCH_RECOGNIZE to find every completed conversion sequence (V -> S -> A -> P) and then aggregates the successful sequences by user_id.

%flink.ssql
SELECT
    T.funnel_user_id,                         -- Selects the user ID from the matched output (T)
    COUNT(*) AS completed_flows_count,        -- Counts the number of matched sequences per group
    COUNT(DISTINCT T.funnel_product_id) AS unique_products_converted -- Counts how many unique products the user converted
FROM clickstream
    MATCH_RECOGNIZE (                         -- Initiates Flink's pattern matching engine
        -- PARTITION BY defines the independent streams for sequence matching
        PARTITION BY user_id, product_id      -- CRITICAL: Sequences are independent for each unique (user, product) pair
        
        -- CHANGE 1: Order by the Rowtime attribute
        ORDER BY ts_ltz                       -- Events within each partition are processed chronologically (by event time)
        
        MEASURES
            -- CRITICAL: Aliased columns for non-ambiguous output
            LAST(P.user_id) AS funnel_user_id,      -- Outputs the user_id from the last event (P) as a non-ambiguous column
            LAST(P.product_id) AS funnel_product_id, -- Outputs the product_id from the last event (P) as a non-ambiguous column
            MATCH_ROWTIME() AS end_time             -- Outputs the event time of the entire match (time of P event)
        
        PATTERN (V S A P)                     -- Defines the exact required sequence: View -> Scroll -> Add -> Purchase
        
        DEFINE
            V AS V.action = 'view',           -- Defines V as any row where the action is 'view'
            S AS S.action = 'scroll',         -- Defines S as any row where the action is 'scroll'
            A AS A.action = 'add_to_cart',    -- Defines A as any row where the action is 'add_to_cart'
            P AS P.action = 'purchase'        -- Defines P as any row where the action is 'purchase'
    ) AS T
GROUP BY T.funnel_user_id;                    -- Groups the matched results to aggregate the counts per user

-- Fraudulent Flow Analysis (Data Quality/Security)
-- This query filters the completed flows from the Total Completed Flows query above based on the time duration between the start (V) and end (P) events, identifying flows completed in $\le 1$ second.

%flink.ssql
SELECT
    T.funnel_user_id,                             -- Selects the user ID from the matched output (T)
    COUNT(*) AS total_fraudulent_flows,           -- Counts the number of flows that meet the WHERE condition (fraudulent)
    MIN(TIMESTAMPDIFF(SECOND, T.start_time, T.end_time)) AS fastest_flow_sec -- Finds the shortest fraudulent flow duration for that user
FROM clickstream
    MATCH_RECOGNIZE (
        PARTITION BY user_id, product_id
        ORDER BY ts_ltz 
        
        MEASURES
            -- CHANGE 1: Extract Rowtime of View event (V)
            V.ts_ltz AS start_time,               -- Outputs the exact event time of the 'view' (V) event
            -- CHANGE 2: Extract Rowtime of Purchase event (P)
            P.ts_ltz AS end_time,                 -- Outputs the exact event time of the 'purchase' (P) event
            
            LAST(P.user_id) AS funnel_user_id,
            LAST(P.product_id) AS funnel_product_id
            
        PATTERN (V S A P)
        
        DEFINE
            V AS V.action = 'view',
            S AS S.action = 'scroll',
            A AS A.action = 'add_to_cart',
            P AS P.action = 'purchase'
    ) AS T
    
-- Filter using the derived Rowtime values (T.start_time and T.end_time)
WHERE TIMESTAMPDIFF(SECOND, T.start_time, T.end_time) <= 1 -- Filters matches where the elapsed time is 1 second or less
GROUP BY T.funnel_user_id;                            -- Aggregates the resulting fraudulent counts per user

-- Tumbling Window (Periodic Totals)
-- This query calculates periodic aggregate metrics (views and purchases) over fixed, non-overlapping time intervals.

%flink.ssql
SELECT
    TUMBLE_START(ts_ltz, INTERVAL '3' MINUTE) AS window_start, -- Outputs the starting timestamp of the 30-minute window
    product_id,
    SUM(CASE WHEN action = 'view' THEN 1 ELSE 0 END) AS total_views,      -- Counts total 'view' actions in the window
    SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS total_purchases -- Counts total 'purchase' actions in the window
FROM clickstream
GROUP BY 
    product_id,
    TUMBLE(ts_ltz, INTERVAL '3' MINUTE);                             -- Groups data into fixed, 30-minute windows based on the Rowtime column (ts_ltz)

-- Session Window (User Behavior)
-- This query analyzes user engagement by grouping events into distinct sessions, defined by a gap in activity.

%flink.ssql
SELECT
    user_id,
    SESSION_START(ts_ltz, INTERVAL '3' MINUTE) AS session_start, -- Outputs the event time of the first event in the session
    SESSION_END(ts_ltz, INTERVAL '3' MINUTE) AS session_end,   -- Outputs the event time of the last event + the gap (3 min)
    -- Calculate the duration of the session
    TIMESTAMPDIFF(SECOND, SESSION_START(ts_ltz, INTERVAL '3' MINUTE), SESSION_END(ts_ltz, INTERVAL '3' MINUTE)) AS session_duration_sec
FROM clickstream
GROUP BY 
    user_id,
    SESSION(ts_ltz, INTERVAL '3' MINUTE);                        -- Groups events into sessions for each user, with a 3-minute inactivity gap

-- 6. Iceberg on S3 (AWS Glue catalog, S3 warehouse)
-- The warehouse bucket comes from S3_BUCKET in .env (region from AWS_REGION). Credentials come from the container AWS_* env vars.
-- This paragraph uses the Scala %flink interpreter because %flink.ssql cannot read environment variables.
-- Avoid ${...} here: Zeppelin treats it as a dynamic form and replaces it with an empty string.

%flink
val warehouse = "s3://<REPLACE ME WITH S3_BUCKET>/warehouse"
stenv.executeSql(
  "CREATE CATALOG glue_catalog WITH (" +
  "'type' = 'iceberg', " +
  "'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog', " +
  "'warehouse' = '" + warehouse + "', " +
  "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO')")

-- Optional. If needed, unregister glue_catalog from the session. Run the catalog paragraph above again to use Iceberg afterwards.

%flink
stenv.asInstanceOf[org.apache.flink.table.api.internal.TableEnvironmentImpl]
  .getCatalogManager.unregisterCatalog("glue_catalog", true)


-- Clean start (optional, destructive): cancel the running Flink jobs in the Flink dashboard first.
-- Drops the Iceberg tables (their data files in S3 should be deleted too; check S3 afterwards, see notes.md "Clean start"), then the Glue database.
-- Flink 1.17 has no DROP CATALOG; the paragraph after the drops unregisters glue_catalog instead.

%flink.ssql
DROP TABLE IF EXISTS glue_catalog.clickstream_db.clickstream_events;
DROP TABLE IF EXISTS glue_catalog.clickstream_db.fraudulent_transactions;
DROP DATABASE IF EXISTS glue_catalog.clickstream_db;

-- Clean start for the Flink session tables (Kafka topics are not deleted).

%flink.ssql
DROP TABLE IF EXISTS fraudulent_transactions_source;
DROP TABLE IF EXISTS fraudulent_transactions;

%flink.ssql
CREATE DATABASE IF NOT EXISTS glue_catalog.clickstream_db;

%flink.ssql
CREATE TABLE IF NOT EXISTS glue_catalog.clickstream_db.clickstream_events (
    user_id STRING,
    product_id STRING,
    action STRING,
    event_time TIMESTAMP_LTZ(3)
);

-- 7. Fraudulent transactions
-- A flow is fraudulent when view, scroll, add to cart and purchase complete within 1 second.

-- Kafka sink: one record per fraudulent flow to the fraudulent-transactions topic.
-- The aggregate fraud query in section 5 is not used here because a Kafka sink can only take an append-only stream.

%flink.ssql
CREATE TABLE IF NOT EXISTS fraudulent_transactions (
    user_id STRING,
    product_id STRING,
    start_time TIMESTAMP_LTZ(3),
    end_time TIMESTAMP_LTZ(3),
    duration_sec BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraudulent-transactions',
    'properties.bootstrap.servers' = 'broker:29092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Fraudulent transactions to Iceberg tables (the source table reads back the fraudulent-transactions Kafka topic)

%flink.ssql
CREATE TABLE IF NOT EXISTS fraudulent_transactions_source (
    user_id STRING,
    product_id STRING,
    start_time TIMESTAMP_LTZ(3),
    end_time TIMESTAMP_LTZ(3),
    duration_sec BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraudulent-transactions',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'fraud_iceberg_writer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

%flink.ssql
CREATE TABLE IF NOT EXISTS glue_catalog.clickstream_db.fraudulent_transactions (
    user_id STRING,
    product_id STRING,
    start_time TIMESTAMP_LTZ(3),
    end_time TIMESTAMP_LTZ(3),
    duration_sec BIGINT
);

-- Streaming jobs (run these last, after all the tables above exist)
-- The three INSERT INTO statements are submitted as one Flink job from a single paragraph (runAsOne).
-- Separate INSERT paragraphs left the second one Pending in Zeppelin.
-- Iceberg commits data on checkpoint, so checkpointing must be enabled for the job submitted from Zeppelin.

%flink.ssql
SET 'execution.checkpointing.interval' = '10s';

%flink.ssql(runAsOne=true)
-- 1. clickstream to Iceberg
INSERT INTO glue_catalog.clickstream_db.clickstream_events
SELECT user_id, product_id, action, ts_ltz AS event_time
FROM default_catalog.default_database.clickstream;

-- 2. fraudulent flows to the fraudulent-transactions Kafka topic
INSERT INTO fraudulent_transactions
SELECT
    T.user_id,
    T.product_id,
    T.start_time,
    T.end_time,
    TIMESTAMPDIFF(SECOND, T.start_time, T.end_time) AS duration_sec
FROM clickstream
    MATCH_RECOGNIZE (
        PARTITION BY user_id, product_id
        ORDER BY ts_ltz
        MEASURES
            V.ts_ltz AS start_time,
            P.ts_ltz AS end_time
        PATTERN (V S A P)
        DEFINE
            V AS V.action = 'view',
            S AS S.action = 'scroll',
            A AS A.action = 'add_to_cart',
            P AS P.action = 'purchase'
    ) AS T
WHERE TIMESTAMPDIFF(SECOND, T.start_time, T.end_time) <= 1;

-- 3. the fraudulent-transactions topic to Iceberg
INSERT INTO glue_catalog.clickstream_db.fraudulent_transactions
SELECT user_id, product_id, start_time, end_time, duration_sec
FROM default_catalog.default_database.fraudulent_transactions_source;
