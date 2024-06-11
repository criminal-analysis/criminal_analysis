from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import pandas as pd
import os

# DAG 기본 설정
default_args = {
    'owner': 'airflow',  # DAG 소유자
    'start_date': days_ago(1),  # DAG 시작 날짜 (하루 전)
    'retries': 1,  # 실패 시 재시도 횟수
}

# DAG 정의
dag = DAG(
    'api_to_redshift',
    default_args=default_args,
    description='Fetch data from API, process it and load into Redshift',  # DAG 설명
    schedule_interval='0 */1 * * *'  # 1시간마다 실행
)

# 데이터 수집 함수
def fetch_data_from_api(**kwargs):
    url = "https://api.odcloud.kr/api/15054711/v1/uddi:9097ad1f-3471-42c6-a390-d85b5121816a?page=1&perPage=2051&serviceKey=uv1CGrcs7xDLOX6aDWvgU5%2FQvBRplsldPgHf9UdAExohcgcS0TxTcCdqUhk5ugNP7ZLbtBssQdsS%2BmvKipPmeQ%3D%3D"
    response = requests.get(url)  # API 호출
    data = response.json()  # JSON 응답 파싱
    
    df = pd.DataFrame(data['data'])  # 데이터프레임 생성
    file_path = '/tmp/raw_data.csv'  # 파일 경로 설정
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # 디렉토리 생성
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"Raw data saved to {file_path}")

# 데이터 가공 함수
def process_data(**kwargs):
    raw_file_path = '/tmp/raw_data.csv'
    processed_file_path = '/tmp/processed_data.csv'
    
    df = pd.read_csv(raw_file_path, encoding='utf-8-sig')
    
    # 경찰서별로 그룹화하고 개수 세기
    grouped_df = df.groupby('경찰서').size().reset_index(name='count')
    
    grouped_df.to_csv(processed_file_path, index=False, encoding='utf-8-sig')
    print(f"Processed data saved to {processed_file_path}")

# S3로 데이터 업로드 함수
def upload_to_s3(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_default')  # S3 연결
    s3.load_file(
        filename='/tmp/processed_data.csv',  # 업로드할 파일 경로
        key='upload/processed_data.csv',  # S3 키
        bucket_name='litchiimg',  # S3 버킷 이름
        replace=True  # 파일 교체
    )
    print(f"File uploaded to s3://litchiimg/upload/processed_data.csv")

# 테이블 생성 SQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS public.police_station_count (
    경찰서 VARCHAR(256),
    count INTEGER
);
"""

# 태스크 정의
fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',  # 태스크 ID
    python_callable=fetch_data_from_api,  # 실행할 Python 함수
    dag=dag,  # 연결된 DAG
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='redshift_dev_db',  # Redshift 연결 ID
    sql=create_table_sql,  # 실행할 SQL
    dag=dag,
)

load_to_redshift_task = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    s3_bucket='litchiimg',  # S3 버킷
    s3_key='upload/processed_data.csv',  # S3 키
    schema='public',  # Redshift 스키마
    table='police_station_count',  # Redshift 테이블
    copy_options=['csv', 'IGNOREHEADER 1'],  # COPY 옵션
    aws_conn_id='aws_default',  # AWS 연결 ID
    redshift_conn_id='redshift_dev_db',  # Redshift 연결 ID
    dag=dag,
)

# 태스크 순서 설정
fetch_data_task >> process_data_task >> upload_to_s3_task >> create_table_task >> load_to_redshift_task
