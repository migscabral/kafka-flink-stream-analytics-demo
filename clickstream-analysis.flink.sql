%flink.ssql
DROP TABLE clickstream;

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
    'properties.bootstrap.servers' = 'broker:9092',
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
-- This query filters the completed flows from Query 4 based on the time duration between the start (V) and end (P) events, identifying flows completed in $\le 1$ second.

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
    TUMBLE_START(ts_ltz, INTERVAL '30' MINUTE) AS window_start, -- Outputs the starting timestamp of the 30-minute window
    product_id,
    SUM(CASE WHEN action = 'view' THEN 1 ELSE 0 END) AS total_views,      -- Counts total 'view' actions in the window
    SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS total_purchases -- Counts total 'purchase' actions in the window
FROM clickstream
GROUP BY 
    product_id,
    TUMBLE(ts_ltz, INTERVAL '30' MINUTE);                             -- Groups data into fixed, 30-minute windows based on the Rowtime column (ts_ltz)

-- Session Window (User Behavior)
-- This query analyzes user engagement by grouping events into distinct sessions, defined by a gap in activity.

%flink.ssql
SELECT
    user_id,
    SESSION_START(ts_ltz, INTERVAL '10' MINUTE) AS session_start, -- Outputs the event time of the first event in the session
    SESSION_END(ts_ltz, INTERVAL '10' MINUTE) AS session_end,   -- Outputs the event time of the last event + the gap (10 min)
    -- Calculate the duration of the session
    TIMESTAMPDIFF(SECOND, SESSION_START(ts_ltz, INTERVAL '10' MINUTE), SESSION_END(ts_ltz, INTERVAL '10' MINUTE)) AS session_duration_sec
FROM clickstream
GROUP BY 
    user_id,
    SESSION(ts_ltz, INTERVAL '10' MINUTE);                        -- Groups events into sessions for each user, with a 10-minute inactivity gap