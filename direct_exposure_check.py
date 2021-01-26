import os
import time


from google.cloud import bigquery

from lifespan import Lifespan
from constants import DELIM, DE_LIFESPAN_DATA, DEFAULT_END_DATE, DEFAULT_START_DATE

security_creds = '/Users/radnam/.config/gcloud/application_default_credentials.json'
gcp_project = 'trm-takehome-mandar-r'
dataset = f'{gcp_project}.trm_sample_data'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = security_creds
os.environ['GCLOUD_PROJECT'] = gcp_project

# Define a BQ client
client = bigquery.Client()

# Query to generate valid sender receiver pairs
# Ideally: We would like to sample few thousand.
'''
SELECT 
  sender, 
  receiver 
FROM `trm-takehome-mandar-r.trm_sample_data.transactions` 
where RAND() < 100/26742011 
'''


def query_agg(use_lifespan=True):
    """
    Average processing time (over 100 queries) = 1.2s (Using address lifespan)
    Average processing time (over 100 queries) = 4.7s (w/o using address lifespan)
    """
    table = 'daily_aggregate_view'
    fname = 'data/direct_exposure_100_no_lifespan.csv'

    if use_lifespan:
        # use different random dataset to avoid internal
        # bq caching affect perf numbers
        fname = 'data/direct_exposure_100_lifespan.csv'
        lifespan = Lifespan()
        lifespan.read_lifespan(DE_LIFESPAN_DATA)

    with open(fname, 'r') as fp:
        next(fp)  # skip header
        start = time.time()
        num_queries = 0
        for line in fp:
            num_queries += 1
            sender, receiver = line.strip().split(DELIM)

            start_ts = DEFAULT_START_DATE
            end_ts = DEFAULT_END_DATE

            if use_lifespan:
                start_ts, end_ts = lifespan.have_overlap(sender, receiver)

            if not start_ts:
                continue

            query = f"""
            SELECT
              value
            FROM
              `{dataset}.{table}`
            WHERE
              (sender = '{sender}' AND receiver = '{receiver}') 
                OR 
              (sender = '{receiver}' AND receiver = '{sender}')
                AND 
              timestamp between '{start_ts}' and '{end_ts}'
            """

            query_job = client.query(query)

            result = query_job.result()
            # Number of rows > 0 and <= Number of days worth of data
            # Since our table generation process guarantees one (sender, receiver)
            # tuple per day
            try:
                assert (0 < result.total_rows)
            except AssertionError:
                print(query)
                break
        end = time.time()
        print(f'Queries: {num_queries}, Average processing time (Upper Bound): {(end - start) / num_queries}')


def query_direct_exposure():
    """
    Average processing time (over 100 queries) = 1.4s
    Performance could be better if we switch to GCP datastore.
    """
    table = 'transactions_direct_exposure'
    fname = 'data/direct_exposure_100.csv'
    with open(fname, 'r') as fp:
        next(fp)  # skip header
        start = time.time()
        num_queries = 0
        for line in fp:
            num_queries += 1
            # Sort the actors to match table creation process
            actor_1, actor_2 = sorted(line.strip().split(DELIM))
            query = f"""
            SELECT
              actor_1, actor_2,
            FROM
              `{dataset}.{table}`
            WHERE
              actor_1 = '{actor_1}' AND actor_2 = '{actor_2}' 
            """
            query_job = client.query(query)
            result = query_job.result()

            assert result.total_rows == 1

        end = time.time()
        print(f'Queries: {num_queries}, Average processing time (Upper Bound): {(end - start) / num_queries}')


if __name__ == "__main__":
    query_agg(use_lifespan=True)
    query_agg(use_lifespan=False)
query_direct_exposure()
