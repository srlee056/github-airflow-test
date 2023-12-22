from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import pandas as pd
import logging
import requests
import json

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_contry_info(url):
    logging.info(datetime.utcnow())
    f = requests.get(url)
    return f.text

@task
def transform(text):
    logging.info("transform started")
    dict_list= json.loads(text)
    records = []
    for dict in dict_list:
        name = dict["name"]["common"].replace("'", "''")
        official_name = dict["name"]["official"].replace("'", "''")
        population = dict["population"]
        area = dict["area"]
        records.append([name, official_name, population, area])
    logging.info("transform done")
    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    name varchar(64),
    official_name varchar(128),
    population int,
    area float
);""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', '{r[1]}', {r[2]}, {r[3]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'GetCountryInfo',
    start_date = datetime(2023,12,13),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:

    url = Variable.get("restcountries_url")
    text = get_contry_info(url)
    results = transform(text)
    load("imsolem1226", "world_info", results)
