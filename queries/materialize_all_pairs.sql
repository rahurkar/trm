-- Generate all Direct Exposures
-- Table size: 323 GB
-- # Rows: 4.7B

with data as (
SELECT
   TIMESTAMP_TRUNC(block_timestamp, DAY) as timestamp,
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
   DATE_TRUNC(DATE(block_timestamp), DAY) between '2009-01-01' AND '2021-01-20'
),
src_dst AS (
select
  (select distinct(string_agg(sr, '_' order by sr))
  from unnest(array[sender, receiver]) sr) as actors,
from
  `data`
),
dst_src as (
select
  (select distinct(string_agg(rs, '_' order by rs))
  from unnest(array[receiver, sender]) rs) as actors,
from
  `data`
),
collect as (
select actors from src_dst
  UNION DISTINCT
select actors from dst_src
)
select
  SPLIT(actors, '_')[OFFSET(0)] as actor_1,
  SPLIT(actors, '_')[OFFSET(1)] as actor_2,
from collect
order by 1