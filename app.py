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
        df['保有開始日'] = pd.to_datetime(df['保有開始日']).dt.date
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
        df['補充日'] = pd.to_datetime(df['補充日']).dt.date
        return df
    except: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日', '補充日', '補充エリア', '確定報酬額'])

# --- データ操作 ---
def add_data_bulk(serials, date):
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    rows = [[str(s), str(date)] for s in serials]
    sheet.append_rows(rows)

def replenish_data_bulk(serials, zone_name, base_price, vol_bonus):
    client = get_connection()
    db_sheet = client.open(SHEET_NAME).sheet1
    hist_sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
    
    all_records = db_sheet.get_all_records()
    df = pd.DataFrame(all_records)
    if df.empty: return 0

    rows_to_delete = []
    history_rows = []
    today = datetime.date.today()
    df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
    
    for s in serials:
        target = df[df['シリアルナンバー'] == str(s)]
        if not target.empty:
            start_date = pd.to_datetime(target.iloc[0]['保有開始日']).date()
            row_idx = target.index[0] + 2
            rows_to_delete.append(row_idx)
            
            days_held = (today - start_date).days
            price = base_price + vol_bonus
            is_early = days_held <= 3
            if is_early: price += 10
            
            history_rows.append([
                str(s), str(start_date), str(today), zone_name, price,
                "早期ボーナス" if is_early else "-"
            ])

    if history_rows:
        hist_sheet.append_rows(history_rows)

    rows_to_delete.sort(reverse=True)
    for r in rows_to_delete:
        db_sheet.delete_rows(r)
        
    return len(rows_to_delete)

def extract_serials(text):
    return re.findall(r'\b\d{8}\b', text)

def get_recommendation(days_held, penalty_left):
    if penalty_left <= 5: return "🔥 即処分"
    elif days_held >= TOKYO_THRESHOLD_DAYS: return "🗼 東京推奨"
    else: return "⚓️ 千葉待機"

# --- メイン処理 ---
def main():
    st.set_page_config(page_title="SpotJobs Manager", layout="wide")

    # --- サイドバー設定 ---
    st.sidebar.header("⚙️ 条件設定")
    zone_keys = list(ZONES.keys())
    selected_zone_name = st.sidebar.selectbox("補充エリア", options=zone_keys, index=0)
    current_base_price = ZONES[selected_zone_name]

    st.sidebar.write("今週のランク")
    vol_level = st.sidebar.select_slider("目標", options=["0-19本", "20-49本", "50-99本", "100-149本", "150本~"], value="0-19本")
    vol_bonus = {"0-19本":0, "20-49本":5, "50-99本":10, "100-149本":15, "150本~":20}[vol_level]
    
    tab_home, tab_inventory, tab_history = st.tabs(["🏠 ホーム", "📦 在庫管理", "💰 週間収益"])
    today = datetime.date.today()

    # ==========================
    # 🏠 ホームタブ
    # ==========================
    with tab_home:
        # 1. 概算
        hist_df = get_history()
        week_earnings = 0
        week_count = 0
        if not hist_df.empty:
            start_of_week = today - datetime.timedelta(days=today.weekday())
            end_of_week = start_of_week + datetime.timedelta(days=6)
            hist_df['補充日'] = pd.to_datetime(hist_df['補充日']).dt.date
            weekly_df = hist_df[(hist_df['補充日'] >= start_of_week) & (hist_df['補充日'] <= end_of_week)]
            week_earnings = weekly_df['確定報酬額'].sum() if not weekly_df.empty else 0
            week_count = len(weekly_df)

        st.markdown("### 📊 今週の成果")
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬概算", f"¥ {week_earnings:,}")
        c2.metric("補充本数", f"{week_count} 本")
        c3.metric("現在単価", f"¥ {current_base_price + vol_bonus}")
        
        st.divider()

        # 2. ジョブ報告
        st.subheader("🚀 ジョブ報告")
        input_text = st.text_area("ジョブ詳細をペースト", height=80, placeholder="バッテリーリスト: ...")
        
        if input_text:
            extracted = list(set(extract_serials(input_text)))
            if extracted:
                st.success(f"🔍 {len(extracted)}本 検出")
                c1, c2 = st.columns(2)
                if c1.button("📥 取出 (追加)", use_container_width=True):
                    add_data_bulk(extracted, today)
                    st.success("✅ 追加・テープを貼ってください！"), st.rerun()
                if c2.button("📤 補充 (確定)", type="primary", use_container_width=True):
                    count = replenish_data_bulk(extracted, selected_zone_name, current_base_price, vol_bonus)
                    if count > 0: st.success(f"🎉 {count}本 確定！"), st.rerun()

        st.divider()

        # 3. 持ち出しリスト (下4桁強調版)
        st.subheader(f"🎒 ピッキングリスト ({STANDARD_RECOMMEND_NUM}選)")
        st.caption("マスキングテープの「日付」と「下4桁」を見てピックアップしてください。")
        
        df = get_data()
        if not df.empty:
            df['経過日数'] = df['保有開始日'].apply(lambda x: (today - x).days)
            df['ペナルティ余命'] = PENALTY_LIMIT_DAYS - df['経過日数']
            
            def calculate_priority(row):
                if row['ペナルティ余命'] <= 5: return 1
                elif row['経過日数'] <= 3: return 2
                return 3
            
            df['優先ランク'] = df.apply(calculate_priority, axis=1)
            df_sorted = df.sort_values(by=['優先ランク', '経過日数'], ascending=[True, False])
            top_n = df_sorted.head(STANDARD_RECOMMEND_NUM)

            if not top_n.empty:
                # コピー用
                st.code(" / ".join(top_n['シリアルナンバー'].tolist()), language="text")

                # カード表示 (下4桁特化)
                cols = st.columns(4)
                for idx, (i, row) in enumerate(top_n.iterrows()):
                    col = cols[idx % 4]
                    with col:
                        p_days = row['ペナルティ余命']
                        serial = row['シリアルナンバー']
                        last4 = serial[-4:] if len(serial) >= 4 else serial
                        
                        if row['優先ランク'] == 1:
                            bg, icon, status = "#ffcccc", "🔥", "即処分"
                        elif row['優先ランク'] == 2:
                            bg, icon, status = "#ccffcc", "💎", "Bonus"
                        else:
                            bg, icon, status = "#f0f2f6", "🐢", "通常"
                        
                        # デザイン: 下4桁を巨大化
                        st.markdown(f"""
                        <div style="background-color:{bg}; padding:10px; border-radius:8px; text-align:center; margin-bottom:10px; border:1px solid #ccc;">
                            <div style="font-size:10px; color:#555;">{status} (あと{p_days}日)</div>
                            <div style="font-size:24px; font-weight:900; letter-spacing:1px; margin:2px 0;">{last4}</div>
                            <div style="font-size:10px; color:#666;">{serial}</div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("在庫なし")
        else:
            st.info("データ取得中...")

    # ==========================
    # 📦 在庫管理タブ
    # ==========================
    with tab_inventory:
        st.subheader("📦 在庫詳細")
        if not df.empty:
            recommend_num = st.selectbox("表示数", list(range(1, 21)), index=6)
            st.markdown(f"**カスタムリスト ({recommend_num}本)**")
            top_custom = df_sorted.head(recommend_num)
            
            # ここも下4桁表示にする？いや、管理用なのでリスト形式で
            display_df = df_sorted[['優先ランク', 'シリアルナンバー', '経過日数', 'ペナルティ余命']]
            def color_coding(row):
                if row['優先ランク'] == 1: return ['background-color: #ffcccc']*len(row)
                if row['優先ランク'] == 2: return ['background-color: #ccffcc']*len(row)
                return ['']*len(row)
            st.dataframe(display_df.style.apply(color_coding, axis=1), use_container_width=True, hide_index=True)

    # ==========================
    # 💰 週間収益タブ
    # ==========================
    with tab_history:
        if not hist_df.empty:
            st.subheader(f"レポート")
            st.dataframe(hist_df.sort_values('補充日', ascending=False), use_container_width=True)

if __name__ == '__main__':
    main()
