import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime

# --- 設定 ---
# スプレッドシートの名前（手順2で付けた名前と完全に一致させる）
SHEET_NAME = 'battery_db'

# スコープ設定
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# --- スプレッドシート接続関数 ---
def get_connection():
    # StreamlitのSecrets（秘密鍵置き場）から認証情報を読み込む
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

def get_data():
    client = get_connection()
    try:
        sheet = client.open(SHEET_NAME).sheet1
        # 全データを取得してDataFrame化
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        # カラムが無い場合の空DF処理
        if df.empty:
            df = pd.DataFrame(columns=['シリアルナンバー', '保有開始日'])
        return df
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"スプレッドシート '{SHEET_NAME}' が見つかりません。共有設定を確認してください。")
        return pd.DataFrame()

def add_data(serial, date):
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    # 行を追加（日付は文字列化）
    sheet.append_row([str(serial), str(date)])

def delete_data(serial):
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    try:
        # シリアルナンバーを検索して削除
        cell = sheet.find(str(serial))
        sheet.delete_rows(cell.row)
        return True
    except:
        return False

# --- メイン処理 ---
def main():
    st.set_page_config(page_title="バッテリー管理（クラウド版）", layout="wide")
    st.title("🔋 バッテリー管理システム (Cloud)")

    # サイドバー：新規登録
    st.sidebar.header("新規登録")
    with st.sidebar.form("add_form"):
        new_serial = st.text_input("シリアルナンバー")
        new_date = st.date_input("保有開始日", datetime.date.today())
        submit_btn = st.form_submit_button("登録")
        
        if submit_btn and new_serial:
            with st.spinner('登録中...'):
                add_data(new_serial, new_date)
            st.success(f"{new_serial} を登録しました")
            st.rerun()

    # データ表示
    df = get_data()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("登録リスト")
        if not df.empty:
            # シリアルナンバーを文字列型として表示（数値扱いによるカンマなどを防ぐ）
            df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.info(f"現在の保有総数: {len(df)} 個")
        else:
            st.write("データがありません。")

    with col2:
        st.subheader("管理メニュー")
        st.write("データの削除")
        if not df.empty:
            delete_serial = st.selectbox("削除するID", df['シリアルナンバー'].unique())
            if st.button("削除実行"):
                with st.spinner('削除中...'):
                    if delete_data(delete_serial):
                        st.warning(f"{delete_serial} を削除しました")
                        st.rerun()
                    else:
                        st.error("削除に失敗しました")

if __name__ == '__main__':
    main()