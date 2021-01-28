# Generated table size for all day ending 2021-01-20
# Table Size: 39G,
# # Rows: 785M
CREATE OR REPLACE table trm_sample_data.actor_active_dates
AS (
WITH
  DATA AS (
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, DAY) AS timestamp,
    input_address AS sender,
    output_address AS receiver,
  FROM
    `bigquery-public-data.crypto_bitcoin.transactions` t,
    UNNEST(inputs) AS input,
    UNNEST(input.addresses) AS input_address,
    UNNEST(outputs) AS output,
    UNNEST(output.addresses) AS output_address
  WHERE
    -- Using partition column to reduce dataset size
    DATE_TRUNC(DATE(block_timestamp), DAY) BETWEEN '2009-01-01' AND '2021-01-20' ),
  sender AS (
  SELECT
    sender AS actor,
    timestamp ts
   FROM
     DATA
),
  receiver AS (
  SELECT
    sender AS actor,
    timestamp ,
  FROM
    DATA
),
  collect AS (
  SELECT
    *
  FROM
    sender
  UNION ALL
  SELECT
    *
  FROM
    receiver )
SELECT
  actor,
  ARRAY_AGG(DISTINCT(ts) ORDER BY ts) ts
FROM
  collect
group by 1
ORDER BY actor
)