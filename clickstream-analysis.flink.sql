-- Time to purchase

SELECT
  user_id,
  product_id,
  duration AS ms_to_purchase
FROM product_clickstream
WHERE action = 'purchase';

-- Step-by-Step Funnel Timing

SELECT
  user_id,
  product_id,
  MIN(CASE WHEN action = 'click' THEN duration END) AS time_to_click,
  MIN(CASE WHEN action = 'add_to_cart' THEN duration END) AS time_to_add_to_cart,
  MIN(CASE WHEN action = 'purchase' THEN duration END) AS time_to_purchase
FROM product_clickstream
GROUP BY user_id, product_id;

-- Detect Bots (too fast actions)
-- If someone “clicks → add_to_cart → purchase” in under 1 second, that’s suspicious.

SELECT
  user_id,
  product_id,
  action,
  duration
FROM product_clickstream
WHERE duration < 1000;

-- Conversion Latency Distribution

SELECT
  product_id,
  action,
  AVG(duration) AS avg_ms,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration) AS median_ms
FROM product_clickstream
GROUP BY product_id, action;

