import pendulum
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)

def aggregate():
    import pymongo
    import pandas as pd
    from pandas_gbq import to_gbq
    from google.cloud import bigquery
    from google.auth import load_credentials_from_file

    MONGO_USERNAME = "test"
    MONGO_PASSWORD = "password"
    MONGO_HOST = "mongo"
    MONGO_PORT = 27017
    MONGO_DB = "tp-note"
    MONGO_COLLECTION = "movies"
    MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"

    client = pymongo.MongoClient(MONGO_URI)
    database = client[MONGO_DB]
    collection = database[MONGO_COLLECTION]

    PROJECT_ID = "bigkuerry"
    DATASET_ID = "posts"
    TABLE_ID = "posts_avg_score"

    CREDENTIALS_PATH = "./data/service-account.json"


    # performing the mongo aggregation
    agg = [
        {
        "$group": {
            "_id": { "$toInt": '$@OwnerUserId' },
            "avgScore": {
            "$avg": { "$toInt": '$@Score' }
            },
            "nbPosts": { "$sum": 1 }
        }
        },
        { "$sort": { "nbPosts": -1 } }
    ]
    result = list(collection.aggregate(agg))
    df = pd.DataFrame(result)

    # connecting to BQ and creating a table from the aggregate result
    credentials, _ = load_credentials_from_file(CREDENTIALS_PATH)
    bq_client = bigquery.Client(PROJECT_ID, credentials)
    schema = [
        bigquery.SchemaField("_id", "INTEGER"),
        bigquery.SchemaField("avgScore", "FLOAT"),
        bigquery.SchemaField("nbPosts", "INTEGER")
    ]
    table = bigquery.Table(bq_client.dataset(DATASET_ID).table(TABLE_ID), schema)
    bq_client.delete_table(table, not_found_ok=True)
    bq_client.create_table(table)
    
    to_gbq(df, destination_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", if_exists='replace', credentials=credentials)

    bq_client.close()
    client.close()


with DAG(
    dag_id="DAG_stackexchange_aggregate_big_query",
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[]
) as dag:
    
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    virtual_classic = PythonVirtualenvOperator(
        task_id="aggregate_big_query",
        requirements=['pymongo', 'pandas', 'pandas-gbq', 'google-cloud-bigquery', 'google-auth'],
        python_callable=aggregate,
    )