import json
import os
import time
from enum import Enum

from google.cloud import bigquery

from constants import DE_LIFESPAN_DATA, DEFAULT_END_DATE, DEFAULT_START_DATE
from lifespan import Lifespan

# BigQuery Config
security_creds = '/Users/radnam/.config/gcloud/application_default_credentials.json'
gcp_project = 'trm-takehome-mandar-r'
dataset = f'{gcp_project}.trm_sample_data'
table = 'daily_aggregate_view'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = security_creds
os.environ['GCLOUD_PROJECT'] = gcp_project

# Define a BQ client
client = bigquery.Client()

# Constants
COUNTER_PARTY = 'counterparty_address'
LIMIT = 100


# As specified in the problem statement.
class FlowType(Enum):
    IN_FLOW = 'inflows'
    OUT_FLOW = 'outflows'
    TOTAL_FLOW = 'total_flows'


def top_flows_optimized(query_address, flow_type=FlowType.TOTAL_FLOW.value, start_date=DEFAULT_START_DATE,
                       end_date=DEFAULT_END_DATE, top_n=LIMIT):
        query = f"""
            WITH data as (
              SELECT
                sender,
                receiver,
                SUM(value) AS value,
                CASE sender
                  WHEN '{query_address}' THEN 1
                  ELSE 0
                END AS outflow,
                CASE receiver
                  WHEN '{query_address}' THEN 1
                  ELSE 0
                END AS inflow,
                CASE sender
                  WHEN '{query_address}' THEN receiver
                  ELSE sender
                 END AS counterparty,
              FROM
                `{dataset}.{table}`
              WHERE
                (sender = '{query_address}' OR receiver = '{query_address}')
                AND timestamp BETWEEN '{start_date}' AND '{end_date}'
              GROUP BY sender, receiver
            )
            SELECT 
              counterparty,
              sum(value*inflow) AS inflows,
              sum(value*outflow) AS outflows,
              sum(value*inflow + value*outflow) AS total_flows,
            FROM data
            GROUP BY counterparty   
            ORDER BY {flow_type} DESC
            LIMIT {top_n}
        """
        query_job = client.query(query)
        result = query_job.result()
        if result.total_rows == 0:
            return None
        data = []
        for r in result:
            data.append(
                {
                    COUNTER_PARTY: r.counterparty,
                    FlowType.IN_FLOW.value: r.inflows,
                    FlowType.OUT_FLOW.value: r.outflows,
                    FlowType.TOTAL_FLOW.value: r.total_flows,
                }
            )
        return json.dumps(data)


def top_flows(query_address, flow_type=FlowType.TOTAL_FLOW.value, start_date=DEFAULT_START_DATE,
              end_date=DEFAULT_END_DATE, top_n=LIMIT):
        query = f"""
        -- top_n = 10 (using limit here, we can also use partition by and row number trick)
        WITH
        outflow AS (
        SELECT
          'outflow' AS type,
          receiver AS counterparty,
          SUM(value) AS value,
        FROM
          `{dataset}.{table}`
        WHERE
          sender = '{query_address}'
           AND timestamp BETWEEN '{start_date}' AND '{end_date}' 
        GROUP BY 1,2
        ORDER BY value
        LIMIT {top_n}
        ),
        inflow AS (
        SELECT
          'inflow' as type,
          sender as counterparty,
          sum(value) as value,
        FROM
          `{dataset}.{table}`
        WHERE
          receiver = '{query_address}'
          AND timestamp BETWEEN '{start_date}' AND '{end_date}' 
        GROUP BY 1,2
        ORDER BY value
        LIMIT {top_n}
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
        LIMIT {top_n}
        )
        SELECT 
          b.counterparty, 
          coalesce(b.value, 0) as total_flows, 
          coalesce(i.value, 0) as inflows, 
          coalesce(o.value, 0) as outflows 
        FROM 
          both b
        FULL OUTER JOIN inflow i on b.counterparty = i.counterparty 
        FULL OUTER JOIN outflow o on b.counterparty = o.counterparty
        ORDER BY {flow_type} DESC
        """
        query_job = client.query(query)
        result = query_job.result()
        if result.total_rows == 0:
            return None
        data = []
        for r in result:
            data.append(
                {
                    COUNTER_PARTY: r.counterparty,
                    FlowType.IN_FLOW.value: r.inflows,
                    FlowType.OUT_FLOW.value: r.outflows,
                    FlowType.TOTAL_FLOW.value: r.total_flows,
                }
            )
        return json.dumps(data)


if __name__ == "__main__":
    fname = 'data/address_200.csv'
    num_queries = 0
    use_lifespan = True
    lifespan = Lifespan()
    lifespan.read_lifespan(DE_LIFESPAN_DATA)

    with open(fname) as fp:
        next(fp)  # Skip header
        start = time.time()
        for line in fp:
            query_address = line.strip()

            start_ts = DEFAULT_START_DATE
            end_ts = DEFAULT_END_DATE

            if use_lifespan:
                start_ts, end_ts = lifespan.get_time(query_address, start_ts, end_ts)

            result = top_flows(query_address, start_date=start_ts, end_date=end_ts)

            # Update query count only we have results
            if result:
                num_queries += 1

    end = time.time()
    print(f'Queries: {num_queries}, Average processing time (Upper Bound): {(end - start) / num_queries}')
