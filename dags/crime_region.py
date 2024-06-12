from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import boto3
import json

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def api_get_upload_s3(url):
    result = requests.get(url).text
    json_object = json.loads(result)
    file_name = 'crime_region_raw_data.json'

    with open(file_name, 'w', encoding="UTF-8") as f:
        json.dump(json_object, f, ensure_ascii=False)

    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    bucket_name = "litchiimg"
    key = "crime_region_raw_data.json"

    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    s3_client.upload_file(file_name, bucket_name, key)
    logging.info("s3 upload done")
    

@task
def extract():
    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    
    # s3 클라이언트 연결
    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    bucket_name = "litchiimg"
    file_name = 'crime_region_raw_data.json'
    res = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = res['Body'].read().decode('utf-8')
    json_obj = json.loads(content)
    
    return json_obj


@task
def transform(obj):
    records = []
    category_dict = {"01": "강력범죄", "02": "절도범죄", "03": "폭력범죄", "07": "마약범죄", "10": "교통범죄"}
    do_dict = {
        "수원시": "경기도", "고양시": "경기도", "용인시": "경기도", "과천시": "경기도", "광명시": "경기도", "광주시": "경기도", "구리시": "경기도", "군포시": "경기도", "김포시": "경기도", "남양주시": "경기도", "동두천시": "경기도", "부천시": "경기도", "성남시": "경기도", "시흥시": "경기도", "안산시": "경기도", "안성시": "경기도", "안양시": "경기도", "양주시": "경기도", "여주시": "경기도", "오산시": "경기도", "의왕시": "경기도", "의정부시": "경기도", "이천시": "경기도", "파주시": "경기도", "평택시": "경기도", "포천시": "경기도", "하남시": "경기도", "화성시": "경기도", # 경기도
        "강릉시": "강원도", "동해시": "강원도", "삼척시": "강원도", "속초시": "강원도", "원주시": "강원도", "춘천시": "강원도", "태백시": "강원도", # 강원도
        "제천시": "충청북도", "청주시": "충청북도", "충주시": "충청북도", # 충청북도
        "계룡시": "충청남도", "공주시": "충청남도", "논산시": "충청남도",
        "당진시": "충청남도", "보령시": "충청남도", "서산시": "충청남도", "아산시": "충청남도", "천안시": "충청남도", # 충청남도
        "군산시": "전라북도", "김제시": "전라북도", "남원시": "전라북도", "익산시": "전라북도", "전주시": "전라북도", "정읍시": "전라북도",
        "광양시": "전라남도", "나주시": "전라남도", "목포시": "전라남도", "순천시": "전라남도", "여수시": "전라남도", # 전라남도
        "포항시": "경상북도", "경주시": "경상북도", "김천시": "경상북도", "안동시": "경상북도", "구미시": "경상북도", # 경상북도
        "영주시": "경상북도", "영천시": "경상북도", "상주시": "경상북도", "문경시": "경상북도", "경산시": "경상북도",
        "창원시": "경상남도", "진주시": "경상남도", "통영시": "경상남도", "사천시": "경상남도", "김해시": "경상남도", "밀양시": "경상남도", "거제시": "경상남도", "양산시": "경상남도", # 경상남도
        "제주시": "제주특별자치도", "서귀포시": "제주특별자치도", # 제주특별자치도
    }
    metropolitan_city = {
        "서울": "서울특별시", "부산": "부산광역시", "광주": "광주광역시", "대구": "대구광역시",
        "인천": "안천광역시", "대전": "대전광역시", "울산": "울산광역시", "세종": "세종특별자치시"
    }
    
    for data in obj:
        occured_date = data["PRD_DE"]
        
        si = data["C2_NM"] # 도시명
        category_code = data["C1"]
        if category_code == "01" or category_code == "03" or si == '기타도시' or si == '도시이외':
            continue
        category = category_dict[category_code[0:2]]
        subcategory = data["C1_NM"]
        crime_count = data["DT"]
        
        # crime_count가 '-' 인 경우
        if crime_count == '-' or crime_count == None:
            crime_count = 0
        
        # 마산이나 진해는 창원으로 통합.
        if si == '마산시' or si == '진해시':
            si = '창원시'
        if si in metropolitan_city:
            do = metropolitan_city[si]
            si = None
        else:
            do = do_dict[si]
        
        records.append(
            [
                int(occured_date),
                do,
                si,
                int(crime_count),
                category,
                subcategory
            ]
        ) # 범죄발생년도, 시도, 시군구, 발생건수, 범죄대분류, 범죄중분류
        
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    """
    records = [
        [ 발생년도, 시도, 시군구, 발생 건 수, 범죄대분류, 범죄중분류 ],
        [ 2022-01-01, 광역자치단체, 서울특별시, 35, 강력범죄, 살인기수],
        [ 2022-01-01, 광역자치단체, 부산광역시, 68, 강력범죄, 살인미수], 
        [ 2022-01-01, 경기도, 고양시, 68, 강력범죄, 살인미수], 
        ...
    ]
    """
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
        occured_date int,
        sido varchar(30),
        sigungu varchar(30),
        crime_count int,
        category varchar(30),
        subcategory varchar(30)
    );"""
    
    cur.execute(create_table_sql)
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            occured_date = r[0]
            sido = r[1]
            sigungu = r[2]
            crime_count = r[3]
            category = r[4]
            subcategory = r[5]
            sql = f"INSERT INTO {schema}.{table} VALUES ('{occured_date}', '{sido}', '{sigungu}', '{crime_count}', '{category}', '{subcategory}')"
            print("insert done")
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
    logging.info("load done")


with DAG(
    dag_id='crime_region',
    start_date=datetime(2024, 6, 10), # 날짜가 미래인 경우 실행이 안됨
    schedule='@yearly', # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:


    url = Variable.get("api_url")
    schema = 'joongh0113'   ## 자신의 스키마로 변경
    table = 'crime_region'
    
    s3 = api_get_upload_s3(url)
    obj = extract()
    records = transform(obj)
    load(schema, table, records)
