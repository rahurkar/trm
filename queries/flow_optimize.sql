# Example address: bc1qzw7g6sns7rcxr7xsgtqwhcpkygyc8l8v39egl5
WITH data as (
  SELECT
    sender,
    receiver,
    SUM(value) AS value,
    CASE sender
      WHEN 'bc1qzw7g6sns7rcxr7xsgtqwhcpkygyc8l8v39egl5' THEN 1
      ELSE 0
     END AS outflow,
    CASE receiver
      WHEN 'bc1qzw7g6sns7rcxr7xsgtqwhcpkygyc8l8v39egl5' THEN 1
      ELSE 0
     END AS inflow,
     CASE sender
      WHEN 'bc1qzw7g6sns7rcxr7xsgtqwhcpkygyc8l8v39egl5' THEN receiver
      ELSE sender
     END AS counterparty,
  FROM
    `trm-takehome-mandar-r.trm_sample_data.daily_aggregate_view`
  WHERE
    (sender = 'bc1qzw7g6sns7rcxr7xsgtqwhcpkygyc8l8v39egl5' OR receiver = 'bc1qzw7g6sns7rcxr7xsgtqwhcpkygyc8l8v39egl5')
    AND timestamp BETWEEN '2021-01-05 00:00:00' AND '2021-01-05 00:00:00'
  GROUP BY sender, receiver
)
SELECT
  counterparty,
  sum(value*inflow) AS inflows,
  sum(value*outflow) AS outflows,
  sum(value*inflow + value*outflow) AS total_flows,
FROM data
GROUP BY counterparty
ORDER BY inflows
LIMIT 10