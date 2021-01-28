-- address: bc1qupuhpz7jzhs9jhhjk286hpc8vjdshsm39hz7ph
-- top_n = 10 (using limit here, we can also use partition by and row number trick)
-- flow_type = inflow
-- start_date:'2021-01-01 00:00:00'
-- end_date:'2021-01-03 00:00:00'
WITH
outflow AS (
SELECT
  'outflow' AS type,
  receiver AS counterparty,
  SUM(value) AS value,
FROM
  `trm-takehome-mandar-r.trm_sample_data.agg_daily_transactions`
WHERE
  sender = 'bc1qupuhpz7jzhs9jhhjk286hpc8vjdshsm39hz7ph'
   AND timestamp BETWEEN '2021-01-01 00:00:00' AND '2021-01-03 00:00:00'
GROUP BY 1,2
ORDER BY value
LIMIT 10
),
inflow AS (
SELECT
  'inflow' as type,
  sender as counterparty,
  sum(value) as value,
FROM
  `trm-takehome-mandar-r.trm_sample_data.agg_daily_transactions`
WHERE
  receiver = 'bc1qupuhpz7jzhs9jhhjk286hpc8vjdshsm39hz7ph'
  AND timestamp BETWEEN '2021-01-01 00:00:00' AND '2021-01-03 00:00:00'
GROUP BY 1,2
ORDER BY value
LIMIT 10
),
both_pre AS (
SELECT outflow.counterparty, value from outflow
UNION ALL
SELECT inflow.counterparty, value from inflow
),
both AS (
SELECT
  'both' AS type,
  counterparty,
  SUM(value) AS value
FROM
  both_pre
GROUP BY 1,2
ORDER BY value
LIMIT 10
)
SELECT
  b.counterparty,
  coalesce(b.value, 0) as both,
  coalesce(i.value, 0) as inflow,
  coalesce(o.value, 0) as outflow
FROM
  both b
FULL OUTER JOIN inflow i on b.counterparty = i.counterparty
FULL OUTER JOIN outflow o on b.counterparty = o.counterparty
ORDER BY both DESC

