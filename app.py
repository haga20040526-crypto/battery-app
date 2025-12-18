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
            
            # 早期ボーナス判定 (3日以内)
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
        # ★ここを修正しました（閉じカッコを追加）
        start_of_week_dt = datetime.datetime.combine(start_of_week, datetime.time.min)
        
        # 日付フィルタ
        weekly_df = hist_df[hist_df['補充日'] >= start_of_week_dt]
        
        week_earnings = weekly_df['確定報酬額'].sum() if not weekly_df.empty else 0
        week_count = len(weekly_df)

    current_bonus = get_vol_bonus(week_count)

    # --- タブ構成 ---
    tab_home, tab_inventory, tab_history = st.tabs(["🏠 ホーム", "📦 在庫管理", "💰 週間収益"])

    # ==========================
    # 🏠 ホームタブ
    # ==========================
    with tab_home:
        # メトリクス表示
        st.markdown("### 📊 今週の成果")
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬概算 (今週)", f"¥ {week_earnings:,}")
        c2.metric("補充本数", f"{week_count} 本")
        
        if current_bonus < 20:
            next_target = 20 if week_count < 20 else (50 if week_count < 50 else (100 if week_count < 100 else 150))
            remain = next_target - week_count
            c3.metric("現在ボーナス", f"+{current_bonus}円", delta=f"あと{remain}本でUP", delta_color="normal")
        else:
            c3.metric("現在ボーナス", f"+{current_bonus}円", "MAXランク到達🎉")
        
        st.divider()

        # ジョブ報告
        st.subheader("🚀 ジョブ報告")
        
        # モード選択
        job_mode = st.radio(
            "作業モードを選択",
            ["📥 取出 (在庫に追加)", "📤 補充 (報酬確定)"],
            horizontal=True
        )

        input_text = st.text_area(
            f"{job_mode}の詳細をペースト", 
            height=80, 
            placeholder="ここにバッテリーリストを貼り付けてください..."
        )
        
        # 日時指定オプション (デフォルトは現在時刻)
        with st.expander("🕒 日時を指定する (通常は現在時刻)"):
            col_d, col_t = st.columns(2)
            input_date = col_d.date_input("日付", value=now.date())
            input_time = col_t.time_input("時間", value=now.time())
            # datetimeオブジェクト作成
            target_dt = datetime.datetime.combine(input_date, input_time)
            target_dt_str = target_dt.strftime('%Y-%m-%d %H:%M:%S')

        if input_text:
            extracted = list(set(extract_serials(input_text)))
            if extracted:
                st.info(f"🔍 {len(extracted)}本 のシリアルナンバーを検出")
                st.caption(f"適用日時: {target_dt_str}")
                
                # --- 取出モード ---
                if job_mode == "📥 取出 (在庫に追加)":
                    if st.button("この内容で在庫登録する", type="primary", use_container_width=True):
                        with st.spinner('登録中...'):
                            add_data_bulk(extracted, target_dt_str)
                        st.success(f"✅ {len(extracted)}本 を在庫に追加しました！")
                        import time
                        time.sleep(1)
                        st.rerun()

                # --- 補充モード ---
                elif job_mode == "📤 補充 (報酬確定)":
                    st.markdown("👇 **条件を指定して確定**")
                    col_zone, col_info = st.columns([2, 1])
                    with col_zone:
                        # デフォルトをDにする
                        default_index = ZONE_OPTIONS.index("D: その他 (船橋など)")
                        selected_zone_name = st.selectbox("今回の補充エリア", ZONE_OPTIONS, index=default_index)
                    
                    base_price = ZONES[selected_zone_name]
                    est_bonus = get_vol_bonus(week_count + len(extracted))
                    est_total_price = base_price + est_bonus
                    
                    with col_info:
                        st.metric("適用単価", f"¥{est_total_price}", f"内訳: {base_price} + {est_bonus}")

                    if st.button("補充を確定する", type="primary", use_container_width=True):
                        with st.spinner('処理中...'):
                            count, applied_bonus = replenish_data_bulk(extracted, selected_zone_name, base_price, week_count, target_dt)
                        if count > 0:
                            st.success(f"🎉 {count}本 確定！ (+{applied_bonus}円ボーナス適用)")
                            import time
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("エラー: 在庫にない番号が含まれている可能性があります")

        st.divider()

        # ピッキングリスト (優先度計算の強化)
        st.subheader(f"🎒 在庫リスト")
        
        df = get_data()
        if not df.empty:
            # 経過時間を計算 (現在時刻 - 保有開始日時)
            df['保有期間'] = df['保有開始日'].apply(lambda x: now - x)
            df['経過日数'] = df['保有期間'].apply(lambda x: x.days)
            df['経過秒'] = df['保有期間'].apply(lambda x: x.total_seconds()) # 秒単位で優先度を出すため
            
            df['ペナルティ余命'] = PENALTY_LIMIT_DAYS - df['経過日数']
            
            def calculate_priority(row):
                if row['ペナルティ余命'] <= 5: return 1
                elif row['経過日数'] <= 3: return 2
                return 3
            
            df['優先ランク'] = df.apply(calculate_priority, axis=1)
            
            # ソート順: 優先ランク(昇順) -> 経過秒(降順=古い順)
            df_sorted = df.sort_values(by=['優先ランク', '経過秒'], ascending=[True, False])
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
                        start_time_str = row['保有開始日'].strftime('%m/%d %H:%M') # 時間まで表示
                        
                        if row['優先ランク'] == 1:
                            bg, icon, status = "#ffcccc", "🔥", "即処分"
                        elif row['優先ランク'] == 2:
                            bg, icon, status = "#ccffcc", "💎", "Bonus"
                        else:
                            bg, icon, status = "#f0f2f6", "🐢", "通常"
                        
                        st.markdown(f"""
                        <div style="background-color:{bg}; padding:10px; border-radius:8px; text-align:center; margin-bottom:10px; border:1px solid #ccc;">
                            <div style="font-size:10px; color:#555;">{status} (あと{p_days}日)</div>
                            <div style="font-size:24px; font-weight:900; letter-spacing:1px; margin:2px 0;">{last4}</div>
                            <div style="font-size:10px; color:#666;">{start_time_str}〜</div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("表示対象なし")
        else:
            st.info("データ読込中...")

        # --- 裏口（一括登録） ---
        st.divider()
        with st.expander("🛠 過去分・手動一括登録 (裏口)"):
            st.caption("指定した日付で強制的に一括登録/補充を行います。時間は「00:00:00」になります。")
            
            col_back_mode, col_back_date = st.columns(2)
            back_mode = col_back_mode.selectbox("処理", ["取出 (過去分追加)", "補充 (過去分完了)"])
            back_date = col_back_date.date_input("日付を指定", value=today)
            
            # 時間指定なしの場合はその日の0時扱い
            back_dt = datetime.datetime.combine(back_date, datetime.time.min)
            back_dt_str = back_dt.strftime('%Y-%m-%d %H:%M:%S')

            back_text = st.text_area("シリアルナンバー (一括)", height=100)
            
            if st.button("裏口実行"):
                if back_text:
                    back_serials = list(set(extract_serials(back_text)))
                    if back_mode == "取出 (過去分追加)":
                        with st.spinner('裏口登録中...'):
                            add_data_bulk(back_serials, back_dt_str)
                        st.success(f"🛠 {len(back_serials)}本 を {back_dt_str} として登録しました")
                        import time
                        time.sleep(1)
                        st.rerun()
                    
                    elif back_mode == "補充 (過去分完了)":
                         # 裏口補充の場合はエリアD固定、ランクなどは現状維持で計算
                        default_d_price = ZONES["D: その他 (船橋など)"]
                        with st.spinner('裏口補充中...'):
                            count, _ = replenish_data_bulk(back_serials, "D: その他 (船橋など)", default_d_price, week_count, back_dt)
                        st.success(f"🛠 {count}本 を {back_dt_str} 付で補充完了にしました")
                        import time
                        time.sleep(1)
                        st.rerun()

    # ==========================
    # 📦 在庫管理タブ
    # ==========================
    with tab_inventory:
        st.subheader("📦 在庫詳細一覧")
        if not df.empty:
            # 時間まで見たいのでフォーマット調整
            df_disp = df_sorted.copy()
            df_disp['保有開始日'] = df_disp['保有開始日'].dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(
                df_disp[['シリアルナンバー', '保有開始日', '経過日数', '優先ランク']], 
                use_container_width=True, 
                hide_index=True
            )

    # ==========================
    # 💰 週間収益タブ
    # ==========================
    with tab_history:
        if not hist_df.empty:
            st.subheader(f"レポート")
            hist_disp = hist_df.sort_values('補充日', ascending=False).copy()
            hist_disp['補充日'] = hist_disp['補充日'].dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(hist_disp, use_container_width=True)

if __name__ == '__main__':
    main()
