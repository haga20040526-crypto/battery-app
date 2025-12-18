import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re

# --- 定数設定 ---
PENALTY_LIMIT_DAYS = 28
SHEET_NAME = 'battery_db' 
HISTORY_SHEET_NAME = 'history'
STANDARD_RECOMMEND_NUM = 7

# --- エリア定義 ---
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

# --- JST日付取得 ---
def get_today_jst():
    now = datetime.datetime.now() + datetime.timedelta(hours=9)
    return now.date()

# --- テキスト解析 ---
def extract_serials_with_date(text, default_date):
    results = []
    default_date_str = default_date.strftime('%Y-%m-%d')

    if "シリアルナンバー" in text:
        blocks = text.split("シリアルナンバー")
        for block in blocks:
            s_match = re.search(r'[:：]?\s*(\d{8})', block)
            if s_match:
                serial = s_match.group(1)
                d_match = re.search(r'(\d{4}-\d{2}-\d{2})', block)
                if d_match:
                    results.append((serial, d_match.group(1)))
                else:
                    results.append((serial, default_date_str))
    else:
        serials = re.findall(r'\b\d{8}\b', text)
        for s in serials:
            results.append((s, default_date_str))
            
    unique_map = {r[0]: r[1] for r in results}
    return list(unique_map.items())

def extract_serials_only(text):
    return list(set(re.findall(r'\b\d{8}\b', text)))

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

def get_vol_bonus(count):
    if count >= 150: return 20
    elif count >= 100: return 15
    elif count >= 50: return 10
    elif count >= 20: return 5
    else: return 0

# --- データ操作 ---
def add_data_bulk_with_dates(data_list):
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    try:
        current_records = sheet.get_all_records()
        existing_map = {str(row['シリアルナンバー']): i for i, row in enumerate(current_records)}
    except:
        existing_map = {}

    rows_to_add = []
    for s, d in data_list:
        if str(s) not in existing_map:
            rows_to_add.append([str(s), str(d)])
    
    if rows_to_add:
        sheet.append_rows(rows_to_add)
        return len(rows_to_add)
    return 0

def replenish_data_bulk(serials, zone_name, base_price, current_week_count, today_date):
    client = get_connection()
    db_sheet = client.open(SHEET_NAME).sheet1
    hist_sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
    
    all_records = db_sheet.get_all_records()
    df = pd.DataFrame(all_records)
    if df.empty: return 0, 0

    rows_to_delete = []
    history_rows = []
    
    df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
    total_count_for_bonus = current_week_count + len(serials)
    vol_bonus = get_vol_bonus(total_count_for_bonus)
    date_str = today_date.strftime('%Y-%m-%d')

    for s in serials:
        target = df[df['シリアルナンバー'] == str(s)]
        if not target.empty:
            start_date = pd.to_datetime(target.iloc[0]['保有開始日']).date()
            row_idx = target.index[0] + 2
            rows_to_delete.append(row_idx)
            days_held = (today_date - start_date).days
            price = base_price + vol_bonus
            is_early = days_held <= 3
            if is_early: price += 10
            
            history_rows.append([
                str(s), str(start_date), date_str, zone_name, price,
                "早期ボーナス" if is_early else "-"
            ])

    if history_rows:
        hist_sheet.append_rows(history_rows)

    rows_to_delete.sort(reverse=True)
    for r in rows_to_delete:
        db_sheet.delete_rows(r)
        
    return len(rows_to_delete), vol_bonus

# --- メイン処理 ---
def main():
    # ページ設定：アイコンもシンプルに
    st.set_page_config(page_title="Battery Manager", page_icon="⚡", layout="wide")
    today = get_today_jst()

    hist_df = get_history()
    week_earnings = 0
    week_count = 0
    
    if not hist_df.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        weekly_df = hist_df[hist_df['補充日'] >= start_of_week]
        week_earnings = weekly_df['確定報酬額'].sum() if not weekly_df.empty else 0
        week_count = len(weekly_df)

    current_bonus = get_vol_bonus(week_count)

    # タブ：絵文字なし、シンプルに
    tab_home, tab_inventory, tab_history = st.tabs(["ホーム", "在庫リスト", "収益レポート"])

    with tab_home:
        st.markdown("### 今週の成果")
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬概算", f"¥ {week_earnings:,}")
        c2.metric("補充本数", f"{week_count} 本")
        
        if current_bonus < 20:
            next_target = 20 if week_count < 20 else (50 if week_count < 50 else (100 if week_count < 100 else 150))
            remain = next_target - week_count
            c3.metric("現在ボーナス", f"+{current_bonus}円", delta=f"あと{remain}本", delta_color="normal")
        else:
            c3.metric("現在ボーナス", f"+{current_bonus}円", "MAX RANK")
        
        st.divider()

        st.subheader("ジョブ登録")
        
        # モード選択：絵文字なし
        job_mode = st.radio(
            "作業モード",
            ["取出 (在庫登録)", "補充 (報酬確定)"],
            horizontal=True
        )

        # --- 取出モード ---
        if job_mode == "取出 (在庫登録)":
            st.caption("「バッテリー管理」画面のリスト全体をコピー＆ペーストしてください")
            default_date = st.date_input("基準日 (読取不可時)", value=today)
            input_text = st.text_area("テキスト貼付", height=150, placeholder="ここにペースト...")
            
            if input_text:
                parsed_data = extract_serials_with_date(input_text, default_date)
                if parsed_data:
                    st.info(f"{len(parsed_data)} 件を検出しました")
                    with st.expander("詳細確認"):
                        st.write(parsed_data)
                    
                    # ボタンにMaterial Iconを使用
                    if st.button("在庫に登録する", type="primary", use_container_width=True, icon=":material/save:"):
                        with st.spinner('処理中...'):
                            count = add_data_bulk_with_dates(parsed_data)
                        if count > 0:
                            st.success(f"{count} 件を登録しました")
                        else:
                            st.warning("検出された番号は既に存在します")
                        import time
                        time.sleep(1.5)
                        st.rerun()

        # --- 補充モード ---
        elif job_mode == "補充 (報酬確定)":
            st.caption("補充したバッテリー番号のリストを貼り付けてください")
            target_date = st.date_input("補充日", value=today)
            input_text = st.text_area("テキスト貼付", height=100, placeholder="ここにペースト...")
            
            if input_text:
                extracted = extract_serials_only(input_text)
                if extracted:
                    st.info(f"{len(extracted)} 件を検出しました")
                    
                    col_zone, col_info = st.columns([2, 1])
                    with col_zone:
                        default_index = ZONE_OPTIONS.index("D: その他 (船橋など)")
                        selected_zone_name = st.selectbox("エリア選択", ZONE_OPTIONS, index=default_index)
                    
                    base_price = ZONES[selected_zone_name]
                    est_bonus = get_vol_bonus(week_count + len(extracted))
                    est_total_price = base_price + est_bonus
                    
                    with col_info:
                        st.metric("適用単価", f"¥{est_total_price}", f"基準{base_price}+ボ{est_bonus}")

                    if st.button("補充を確定する", type="primary", use_container_width=True, icon=":material/check_circle:"):
                        with st.spinner('処理中...'):
                            count, applied_bonus = replenish_data_bulk(extracted, selected_zone_name, base_price, week_count, target_date)
                        if count > 0:
                            st.success(f"{count} 件の補充を確定しました")
                            import time
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("エラー: 在庫が見つかりません")

        st.divider()

        # --- 在庫カード表示（デザイン刷新） ---
        st.subheader("ピックアップ推奨")
        
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
                st.caption("コピー用:")
                st.code(" / ".join(top_n['シリアルナンバー'].tolist()), language="text")

                cols = st.columns(4)
                for idx, (i, row) in enumerate(top_n.iterrows()):
                    col = cols[idx % 4]
                    with col:
                        p_days = row['ペナルティ余命']
                        serial = row['シリアルナンバー']
                        last4 = serial[-4:] if len(serial) >= 4 else serial
                        start_date_str = row['保有開始日'].strftime('%m/%d')
                        
                        # デザイン定義: シンプルな色分けとフラットデザイン
                        if row['優先ランク'] == 1:
                            # 期限切れ間近 (赤)
                            border_color = "#e57373"
                            bg_color = "#ffebee"
                            status_text = f"要返却 (残{p_days}日)"
                            text_color = "#c62828"
                        elif row['優先ランク'] == 2:
                            # ボーナス期間 (緑)
                            border_color = "#81c784"
                            bg_color = "#e8f5e9"
                            status_text = "Bonus期間"
                            text_color = "#2e7d32"
                        else:
                            # 通常 (グレー)
                            border_color = "#e0e0e0"
                            bg_color = "#fafafa"
                            status_text = f"通常 (残{p_days}日)"
                            text_color = "#616161"
                        
                        # HTML/CSSでモダンなカードを作成
                        st.markdown(f"""
                        <div style="
                            background-color: white;
                            border-radius: 8px;
                            border-left: 6px solid {border_color};
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                            padding: 12px;
                            margin-bottom: 12px;
                        ">
                            <div style="font-size: 11px; font-weight: bold; color: {text_color}; text-transform: uppercase; margin-bottom: 4px;">
                                {status_text}
                            </div>
                            <div style="font-size: 26px; font-weight: 800; color: #333; letter-spacing: 1px; line-height: 1.2;">
                                {last4}
                            </div>
                            <div style="display: flex; justify-content: space-between; align-items: end; margin-top: 4px;">
                                <div style="font-size: 10px; color: #999;">{serial}</div>
                                <div style="font-size: 12px; font-weight: 600; color: #555;">{start_date_str}〜</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("表示対象なし")
        else:
            st.info("データを読み込んでいます...")

    with tab_inventory:
        st.subheader("在庫詳細")
        if not df.empty:
            df_disp = df_sorted.copy()
            df_disp['保有開始日'] = df_disp['保有開始日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # シンプルなテーブル表示
            st.dataframe(
                df_disp[['シリアルナンバー', '保有開始日', '経過日数']], 
                use_container_width=True, 
                hide_index=True
            )

    with tab_history:
        st.subheader("収益レポート")
        if not hist_df.empty:
            hist_disp = hist_df.sort_values('補充日', ascending=False).copy()
            hist_disp['補充日'] = hist_disp['補充日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(hist_disp, use_container_width=True)

if __name__ == '__main__':
    main()
