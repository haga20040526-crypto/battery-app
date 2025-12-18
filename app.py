import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re

# --- 定数設定 ---
PENALTY_LIMIT_DAYS = 28
TOKYO_THRESHOLD_DAYS = 14
SHEET_NAME = 'battery_db' 
HISTORY_SHEET_NAME = 'history'
STANDARD_RECOMMEND_NUM = 7

# --- エリア定義 ---
# 辞書の順番に関わらず、Dをデフォルトにするためのリスト
ZONE_OPTIONS = [
    "D: その他 (船橋など)", 
    "A: 東京23区", 
    "B: 東京都下", 
    "C: 指定都市(横浜等)"
]
ZONES = {
    "D: その他 (船橋など)": 70,
    "A: 東京23区": 55,
    "B: 東京都下": 65,
    "C: 指定都市(横浜等)": 60,
}

# --- GCP設定 ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_connection():
    if "gcp_service_account" not in st.secrets:
        st.error("Secretsの設定が見つかりません。")
        return None
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

# --- データ取得 ---
def get_data():
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        sheet = client.open(SHEET_NAME).sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        if df.empty: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日'])
        
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        # 日付だけでなく時間も含めて変換
        df['保有開始日'] = pd.to_datetime(df['保有開始日'])
        return df
    except: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日'])

def get_history():
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        if df.empty: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日', '補充日', '補充エリア', '確定報酬額'])
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        df['補充日'] = pd.to_datetime(df['補充日'])
        return df
    except: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日', '補充日', '補充エリア', '確定報酬額'])

# --- ボーナス計算ロジック ---
def get_vol_bonus(count):
    if count >= 150: return 20
    elif count >= 100: return 15
    elif count >= 50: return 10
    elif count >= 20: return 5
    else: return 0

# --- データ操作 ---
def add_data_bulk(serials, timestamp_str):
    """
    timestamp_str: 'YYYY-MM-DD HH:MM:SS' 形式の文字列
    """
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    # 時間まで保存
    rows = [[str(s), str(timestamp_str)] for s in serials]
    sheet.append_rows(rows)

def replenish_data_bulk(serials, zone_name, base_price, current_week_count, timestamp_dt):
    """
    timestamp_dt: datetimeオブジェクト
    """
    client = get_connection()
    db_sheet = client.open(SHEET_NAME).sheet1
    hist_sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
    
    all_records = db_sheet.get_all_records()
    df = pd.DataFrame(all_records)
    if df.empty: return 0

    rows_to_delete = []
    history_rows = []
    
    df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
    
    total_count_for_bonus = current_week_count + len(serials)
    vol_bonus = get_vol_bonus(total_count_for_bonus)

    for s in serials:
        target = df[df['シリアルナンバー'] == str(s)]
        if not target.empty:
            start_dt = pd.to_datetime(target.iloc[0]['保有開始日'])
            
            # 行番号取得 (pandas index + 2)
            row_idx = target.index[0] + 2
            rows_to_delete.append(row_idx)
            
            # 経過日数計算 (時間差を考慮)
            time_diff = timestamp_dt - start_dt
            days_held = time_diff.days
            
            price = base_price + vol_bonus
            
            # 早期ボーナス判定 (3日以内 = 72時間以内 と解釈もできるが、既存ロジックに合わせて日数で判定)
            is_early = days_held <= 3
            if is_early: price += 10
            
            history_rows.append([
                str(s), 
                str(start_dt), 
                str(timestamp_dt), # 時間付きで保存
                zone_name, 
                price,
                "早期ボーナス" if is_early else "-"
            ])

    if history_rows:
        hist_sheet.append_rows(history_rows)

    rows_to_delete.sort(reverse=True)
    for r in rows_to_delete:
        db_sheet.delete_rows(r)
        
    return len(rows_to_delete), vol_bonus

def extract_serials(text):
    return re.findall(r'\b\d{8}\b', text)

# --- メイン処理 ---
def main():
    st.set_page_config(page_title="SpotJobs Manager", layout="wide")
    
    # 現在時刻 (秒まで)
    now = datetime.datetime.now()
    today = now.date()

    # --- 1. 週次データの集計 (月曜始まり) ---
    hist_df = get_history()
    week_earnings = 0
    week_count = 0
    
    if not hist_df.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday()) # 今週の月曜
        start_of_week_dt = datetime.datetime.combine(start_of_week, datetime.time.min
