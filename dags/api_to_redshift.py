from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging

api_url = "https://api.odcloud.kr/api/15054711/v1/uddi:9097ad1f-3471-42c6-a390-d85b5121816a?page=1&perPage=2051&serviceKey=uv1CGrcs7xDLOX6aDWvgU5%2FQvBRplsldPgHf9UdAExohcgcS0TxTcCdqUhk5ugNP7ZLbtBssQdsS%2BmvKipPmeQ%3D%3D"

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Redshift 연결 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def fetch_and_process_data():
    try:
        response = requests.get(api_url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data from API: {e}")
        return None
    
    data = response.json()
    df = pd.DataFrame(data['data'])

    file_path = '/tmp/raw_data.csv'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    logging.info(f"Raw data saved to {file_path}")

    # 데이터 가공
    police_station_grouped_file_path = '/tmp/police_station_grouped_data.csv'
    city_with_iso_file_path = '/tmp/city_with_iso_data.csv'
    
    police_station_grouped_df = df.groupby('경찰서').size().reset_index(name='count')
    police_station_grouped_df.to_csv(police_station_grouped_file_path, index=False, encoding='utf-8-sig')
    logging.info(f"Police station grouped data saved to {police_station_grouped_file_path}")
    
    df['시도'] = df['시도청'].str.replace('청$', '', regex=True)
    df.loc[df['시도'] == '경기남부', '시도'] = '경기'
    df.loc[df['시도'] == '경기북부', '시도'] = '경기'
    
    city_grouped_df = df.groupby('시도').size().reset_index(name='count')
    iso_code_dict = {
        "부산": "KR-26", "충북": "KR-43", "충남": "KR-44", "대구": "KR-27", "대전": "KR-30", "강원": "KR-42",
        "광주": "KR-29", "경기": "KR-41", "경북": "KR-47", "경남": "KR-48", "인천": "KR-28", "제주": "KR-49",
        "전북": "KR-45", "전남": "KR-46", "세종": "KR-50", "서울": "KR-11", "울산": "KR-31",
    }
    city_grouped_df['iso_code'] = city_grouped_df['시도'].map(iso_code_dict)
    city_grouped_df.to_csv(city_with_iso_file_path, index=False, encoding='utf-8-sig')
    logging.info(f"City with ISO code data saved to {city_with_iso_file_path}")

    return {
        'police_station_grouped_file_path': police_station_grouped_file_path,
        'city_with_iso_file_path': city_with_iso_file_path
    }

@task
def upload_to_s3(paths):
    if paths is None:
        logging.error("No paths to upload")
        return
    
    s3_hook = S3Hook(aws_conn_id='aws_default')

    s3_hook.load_file(paths['police_station_grouped_file_path'], key='upload/police_station_grouped_data.csv', bucket_name='litchiimg', replace=True)
    logging.info("File uploaded to s3://litchiimg/upload/police_station_grouped_data.csv")

    s3_hook.load_file(paths['city_with_iso_file_path'], key='upload/city_with_iso_data.csv', bucket_name='litchiimg', replace=True)
    logging.info("File uploaded to s3://litchiimg/upload/city_with_iso_data.csv")

@task
def create_and_truncate_tables():
    cur = get_Redshift_connection()
    create_police_table_sql = """
    CREATE TABLE IF NOT EXISTS hhee2864.police_station_count (
        경찰서 VARCHAR(256),
        count INTEGER
    );
    """
    create_city_table_sql = """
    CREATE TABLE IF NOT EXISTS hhee2864.city_with_iso_count (
        시도 VARCHAR(256),
        count INTEGER,
        iso_code VARCHAR(10)
    );
    """
    truncate_police_table_sql = "TRUNCATE TABLE hhee2864.police_station_count;"
    truncate_city_table_sql = "TRUNCATE TABLE hhee2864.city_with_iso_count;"
    
    cur.execute(create_police_table_sql)
    cur.execute(create_city_table_sql)
    cur.execute(truncate_police_table_sql)
    cur.execute(truncate_city_table_sql)

@task
def load_to_redshift():
    cur = get_Redshift_connection()
    police_station_copy_sql = """
    COPY hhee2864.police_station_count
    FROM 's3://litchiimg/upload/police_station_grouped_data.csv'
    IAM_ROLE 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/YOUR_REDSHIFT_ROLE'
    CSV
    IGNOREHEADER 1;
    """
    city_with_iso_copy_sql = """
    COPY hhee2864.city_with_iso_count
    FROM 's3://litchiimg/upload/city_with_iso_data.csv'
    IAM_ROLE 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/YOUR_REDSHIFT_ROLE'
    CSV
    IGNOREHEADER 1;
    """
    cur.execute(police_station_copy_sql)
    cur.execute(city_with_iso_copy_sql)

with DAG(
    dag_id='api_to_redshift',
    start_date=datetime(2024, 6, 10),
    schedule_interval='*/10 * * * *',  # 10분마다 실행
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:
    data_paths = fetch_and_process_data()
    s3_upload = upload_to_s3(data_paths)
    create_truncate_tables = create_and_truncate_tables()
    load_data_to_redshift = load_to_redshift()

    data_paths >> s3_upload >> create_truncate_tables >> load_data_to_redshift
