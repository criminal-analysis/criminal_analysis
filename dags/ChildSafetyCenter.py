from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime
from datetime import timedelta

import csv
import json
import logging
import psycopg2
import requests
from requests.exceptions import HTTPError
from http import HTTPStatus

import os
import pandas as pd
import time
from collections import defaultdict

from plugins import s3
from plugins import update_sido


s3_conn_id = "aws_default"
s3_bucket = "litchiimg"
data_dir = Variable.get("DATA_DIR")


def save_raw_data(data, path):
    df = pd.json_normalize(data)
    path = path + "/" + "raw_safety_center_list.csv"
    result = df.to_csv(path, index=False, encoding="UTF-8", quoting=csv.QUOTE_ALL, header=True)

    return path

def download_raw_data(s3_conn_id, s3_bucket, s3_key, path):
    download_path = s3.download_to_s3(s3_conn_id, s3_bucket, s3_key, path)
    origin_path = path + "/" + "raw_safety_center_list.csv"

    if not download_path:
        if os.path.exists(origin_path):
            if os.stat(origin_path).st_mtime < time.time() - 86400:
                os.remove(origin_path)
                download_path = s3.download_to_s3(s3_conn_id, s3_bucket, s3_key, path)
            else:
                download_path = origin_path
    
    return download_path

def update_sido_data(path):
    raw_safety_center_df, local_name_df, zip_code_new_df, zip_code_old_df, phone_number_df = update_sido.load_data(path)

    raw_safety_center_df = update_sido.preprocess_data(raw_safety_center_df)
    raw_safety_center_df = update_sido.extract_city(raw_safety_center_df)
    
    merged_df = update_sido.join_local_name(raw_safety_center_df, local_name_df)
    zip_code_new_df, zip_code_old_df = update_sido.preprocess_zip_codes(zip_code_new_df, zip_code_old_df)

    merged_df = update_sido.update_sido_zip(merged_df, zip_code_new_df, zip_code_old_df)
    merged_df = update_sido.update_sido_local_name(merged_df)
    logging.info(f"sido parsing counts: {merged_df[~merged_df['sido'].isna()].count()}")

    merged_df = update_sido.fill_missing_sido(merged_df)
    logging.info(f"missing counts: {merged_df[merged_df['sido'].isna()].count()}")

    path = update_sido.save_to_csv(merged_df, Variable.get("DATA_DIR") + '/' + 'updated_safety_center_list.csv')
    return path

@task
def extract(url):
    logging.info(datetime.now())

    params = {
        "esntlId": f"{Variable.get('police_api_id')}",
        "authKey": f"{Variable.get('police_api_key')}",
        "pageIndex": "1",
        "pageUnit": "1",
        "detailDate1": "09",
        "xmlUseYN": "N"
    }

    try:
        first_info = requests.post(url, params=params)
        center_info = defaultdict(list)

        if first_info.status_code != HTTPStatus.OK:
            raise first_info.raise_for_status()
        
        first_data = first_info.json()
        totalCount = first_data["totalCount"]

        s3_key = "upload" + "/" + "raw_safety_center_list.csv"
        path = download_raw_data(s3_conn_id, s3_bucket, s3_key, data_dir)
        if not path:
            loopCount = totalCount // 100 + 1
            logging.info(f"loopCount: {loopCount}")
            params["pageUnit"] = 100
            
            for idx in range(1, loopCount + 1):
                time.sleep(1)
                logging.info(f"idx >> {idx}")
                params["pageIndex"] = idx
                data = requests.post(url, params=params)
                data = data.json()["list"]
                center_info["list"].extend(data)
            
            path = save_raw_data(center_info["list"], data_dir)
            s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, [path], True)

    except (HTTPError, Exception) as e:
        logging.error(e)
        raise

    return path

@task
def transform(path):
    logging.info("Transform started")
    records = []

    csv_path = update_sido_data(path)

    s3_key = "upload" + "/" + "updated_safety_center_list.csv"
    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, [csv_path], True)
    
    logging.info("Transform ended")


with DAG(
    dag_id='ChildSafetyCenter',
    start_date=datetime(2024,5,25),
    schedule = '@once',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    url = "https://www.safe182.go.kr/api/lcm/safeMap.do"
    schema = 'hhee2864'
    table_info = {
        "table_name": "child_safety_center",
        "table_schema": [
            "serial_no int primary key",
            "place_name varchar(150) not null",
            "sido varchar(30) not null",
            "sigungu varchar(30)",
            "address varchar(1000)",
            "zip_code varchar(10)",
            "phone_number varchar(20)"
        ]
    }

    table_setting_task = SQLExecuteQueryOperator(
        task_id = 'table_setting_{}_in_redshfit'.format(table_info["table_name"]),
        conn_id = "redshift_dev_db",
        sql = f"""
        DROP TABLE IF EXISTS {table_info["table_name"]};
        CREATE TABLE {table_info["table_name"]} ({",".join(table_info["table_schema"])});""",
        autocommit = True,
        split_statements = True,
        return_last = False,
        dag = dag
    )

    load = S3ToRedshiftOperator(
        task_id = 'run_copy_sql_{}'.format(table_info["table_name"]),
        s3_bucket = s3_bucket,
        s3_key = "upload" + "/" + "updated_safety_center_list.csv",
        schema = schema,
        table = table_info["table_name"],
        column_list = ["serial_no", "place_name", "phone_number", "address", "zip_code", "sigungu", "sido"],
        copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','"],
        method = 'REPLACE',
        redshift_conn_id = "redshift_dev_db",
        aws_conn_id = 'aws_default',
        dag = dag
    )

    extract_raw_data = extract(url) # '/opt/airflow/data/raw_safety_center_list.csv'
    transformed_data = transform('/opt/airflow/data/raw_safety_center_list.csv')

    extract_raw_data >> transformed_data >> table_setting_task >> load