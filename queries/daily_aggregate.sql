-- We generate daily_aggregate_view on this query.
with data as (
SELECT
   TIMESTAMP_TRUNC(block_timestamp, DAY) as timestamp,
   input_address AS sender,
   output_address AS receiver,
   SAFE_DIVIDE(LEAST(input_value, output_value), POW(10, 8)) AS value,
FROM
  `bigquery-public-data.crypto_bitcoin.transactions` t,
  UNNEST(inputs) AS input,
  UNNEST(input.addresses) AS input_address,
  UNNEST(outputs) AS output,
  UNNEST(output.addresses) AS output_address
WHERE
  -- Using partition column to reduce dataset size
   TIMESTAMP_TRUNC(block_timestamp, DAY) between '2009-01-01' AND '2021-01-20'
),
data1 as (
SELECT
  sender,
  receiver,
  timestamp,
  SUM(value) as value
FROM
  `data`
GROUP BY
  1, 2, 3
)
select * from data1