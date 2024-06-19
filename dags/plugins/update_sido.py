from airflow.models import Variable

import pandas as pd
import re
import csv

# 파일 경로 설정
data_dir = Variable.get("DATA_DIR")

local_name_path = data_dir + '/localname_list.csv'
zip_code_new_path = data_dir + '/zipcode_new.csv'
zip_code_old_path = data_dir + '/zipcode_old.csv'

def load_data(safety_center_path=data_dir + '/raw_safety_center_list.csv'):
    safety_center_df = pd.read_csv(safety_center_path)
    local_name_df = pd.read_csv(local_name_path)
    zip_code_new_df = pd.read_csv(zip_code_new_path)
    zip_code_old_df = pd.read_csv(zip_code_old_path)
    return (safety_center_df, local_name_df, zip_code_new_df, zip_code_old_df)

def preprocess_data(safety_center_df):
    # NaN 값을 빈 문자열로 대체
    safety_center_df['adres'] = safety_center_df['adres'].fillna('')

    # 명칭 앞뒤 공백 제거
    safety_center_df["bsshNm"] = safety_center_df["bsshNm"].str.strip()

    # 주소 앞뒤 공백 제거
    safety_center_df["adres"] = safety_center_df["adres"].str.replace(",", " ", 1).str.strip()
    return safety_center_df

def extract_city(safety_center_df):
    # 시/군/구 정보 추출을 위한 정규식 패턴
    district_pattern = re.compile(r'([가-힣]+?[시군구])')

    # 'adres'에서 시/군/구 정보를 추출하여 새로운 컬럼 'city'에 저장
    def extract_city_from_adres(adres):
        match = district_pattern.search(adres)
        return match.group(1) if match else None

    safety_center_df['city'] = safety_center_df['adres'].apply(extract_city_from_adres)
    return safety_center_df

def join_local_name(safety_center_df, local_name_df):
    # city 컬럼을 기준으로 조인하여 시도 정보 추가
    merged_df = safety_center_df.merge(local_name_df, left_on='city', right_on='local_name', how='left')

    # 중복 행 예외 처리 추가
    merged_df.drop_duplicates(subset=['lcSn'], keep='first', inplace=True, ignore_index=True)
    return merged_df

def preprocess_zip_codes(zip_code_new_df, zip_code_old_df):
    # 우편번호 규칙 정제 함수
    def preprocess(df):
        df = df.assign(zip_code=df.zip_code.str.split(',')).explode('zip_code')
        df['zip_code'] = df['zip_code'].str.strip()
        return df

    zip_code_new_df = preprocess(zip_code_new_df)
    zip_code_old_df = preprocess(zip_code_old_df)
    return (zip_code_new_df, zip_code_old_df)

def update_sido_by_zip(row, zip_code_new_df, zip_code_old_df):
    zip_code = str(row['zip']) if not pd.isna(row['zip']) else ''

    # 구 우편번호인 경우
    if len(zip_code) >= 6:
        matched_sido = zip_code_old_df[zip_code_old_df['zip_code'] == zip_code[:2]]
        if zip_code[:3] == "339":
            matched_sido = zip_code_old_df[zip_code_old_df['zip_code'] == zip_code[:3]]
    # 신 우편번호인 경우
    elif len(zip_code) == 5 and '-' not in zip_code:
        matched_sido = zip_code_new_df[zip_code_new_df['zip_code'] == zip_code[:2]]
    else:
        return row['sido']

    if not matched_sido.empty:
        return matched_sido['sido'].values[0]
    return row['sido']

def update_sido_zip(merged_df, zip_code_new_df, zip_code_old_df):
    merged_df['sido'] = merged_df.apply(update_sido_by_zip, axis=1, args=(zip_code_new_df, zip_code_old_df))
    return merged_df

def correct_local_name(row, full_district_name_list, short_district_name_list):
    def get_full_district_index(address):
        for i, district in enumerate(full_district_name_list):
            if address.startswith(district):
                return i
        return None

    if row['city'] == '광주시' and '경기도' in row['adres']:
        return '경기'
    elif row['city'] == '광주시' or row['city'] == '광주광역시' and '광주광역시' in row['adres']:
        return '광주'

    # short_district_name_list에서 일치하는 항목을 찾으면 해당 항목 반환
    for short_name in short_district_name_list:
        if row['adres'].startswith(short_name):
            return short_name

    final_result = get_full_district_index(row['adres'])
    return row['sido'] if final_result is None else short_district_name_list[final_result]

def update_sido_local_name(merged_df):
    full_district_name_list = ['서울특별시','부산광역시','대전광역시','대구광역시','울산광역시','인천광역시','세종특별자치시','경기도','강원도','충청북도','충청남도','전라북도','전라남도','경상북도','경상남도','제주특별자치도']
    short_district_name_list = ['서울','부산','대전','대구','울산','인천','세종','경기','강원','충북','충남','전북','전남','경북','경남','제주']

    merged_df['sido'] = merged_df.apply(correct_local_name, axis=1, args=(full_district_name_list, short_district_name_list))
    return merged_df

def fill_missing_sido(merged_df):
    # 모든 주소를 확인할 수 없기에 부득이하게 결측값 처리
    merged_df['sido'] = merged_df['sido'].fillna('주소불명')
    return merged_df

def get_local_rows_count(path=data_dir + '/raw_safety_center_list.csv'):
    try:
        rows = len(pd.read_csv(path))
    except:
        rows = 0
    
    return rows

def save_to_csv(merged_df, output_path):
    # 최종 파일 저장
    merged_df.columns = ["drop0","serial_no","place_name","phone_number","address","drop1","zip_code","drop2","drop3","drop4","drop5","drop6","drop7","drop8","sigungu","drop9","sido"]
    merged_df.drop(columns=["drop0","drop1","drop2","drop3","drop4","drop5","drop6","drop7","drop8","drop9"], inplace=True)

    result = merged_df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL, header=True)
    return output_path if result is None else result

if __name__ == "__main__":
    raw_safety_center_df, local_name_df, zip_code_new_df, zip_code_old_df = load_data()

    raw_safety_center_df = preprocess_data(raw_safety_center_df)
    raw_safety_center_df = extract_city(raw_safety_center_df)

    merged_df = join_local_name(raw_safety_center_df, local_name_df)

    zip_code_new_df, zip_code_old_df = preprocess_zip_codes(zip_code_new_df, zip_code_old_df)

    merged_df = update_sido_zip(merged_df, zip_code_new_df, zip_code_old_df)
    merged_df = update_sido_local_name(merged_df)
    merged_df = fill_missing_sido(merged_df)

    save_to_csv(merged_df, data_dir + '/updated_safety_center_list.csv')
