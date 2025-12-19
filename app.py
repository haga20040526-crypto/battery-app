import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import altair as alt

# --- 定数設定 ---
PENALTY_LIMIT_DAYS = 28
SHEET_NAME = 'battery_db' 
HISTORY_SHEET_NAME = 'history'

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
        df['保有開始日'] = pd.to_datetime(df['保有開始日'], errors='coerce').dt.date
        df = df.dropna(subset=['保有開始日']) 
        return df
    except: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日'])

def get_history():
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        expected_cols = ['シリアルナンバー', '保有開始日', '補充日', '補充エリア', '確定報酬額', '備考']
        if df.empty: return pd.DataFrame(columns=expected_cols)
        
        df['確定報酬額'] = pd.to_numeric(df['確定報酬額'], errors='coerce').fillna(0).astype(int)
        df['補充日'] = pd.to_datetime(df['補充日'], errors='coerce').dt.date
        return df
    except: return pd.DataFrame(columns=['シリアルナンバー', '保有開始日', '補充日', '補充エリア', '確定報酬額', '備考'])

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
    skipped_count = 0
    
    for s, d in data_list:
        if str(s) not in existing_map:
            rows_to_add.append([str(s), str(d)])
        else:
            skipped_count += 1
    
    if rows_to_add:
        sheet.append_rows(rows_to_add)
    
    return len(rows_to_add), skipped_count

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

def add_manual_history(date_obj, amount, memo, category):
    client = get_connection()
    hist_sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
    date_str = date_obj.strftime('%Y-%m-%d')
    row = [category, "-", date_str, "-", amount, memo]
    hist_sheet.append_row(row)

# --- ★修正版: カード表示用HTML生成関数 ---
def create_card_html(row, today):
    p_days = PENALTY_LIMIT_DAYS - (today - row['保有開始日']).days
    days_held = (today - row['保有開始日']).days
    serial = row['シリアルナンバー']
    last4 = serial[-4:] if len(serial) >= 4 else serial
    start_date_str = row['保有開始日'].strftime('%m/%d')
    
    # 優先度計算
    if p_days <= 5: priority = 1
    elif days_held <= 3: priority = 2
    else: priority = 3

    if priority == 1:
        # 赤 (要返却)
        border, text_c, status = "#e57373", "#c62828", f"🔥 要返却 (残{p_days}日)"
        bg_c = "#fff5f5"
    elif priority == 2:
        # 緑 (Bonus)
        border, text_c, status = "#81c784", "#2e7d32", "💎 Bonus期間"
        bg_c = "#f1f8e9"
    else:
        # 通常
        border, text_c, status = "#bdbdbd", "#616161", f"🐢 通常 (残{p_days}日)"
        bg_c = "#ffffff"
    
    # デザイン刷新: 日付をメインに、SNを小さく
    return f"""
    <div style="
        background-color: {bg_c}; 
        border-radius: 8px; 
        border-left: 8px solid {border}; 
        box-shadow: 0 2px 5px rgba(0,0,0,0.1); 
        padding: 12px; 
        margin-bottom: 12px;
    ">
        <div style="font-size: 12px; font-weight: bold; color: {text_c}; margin-bottom: 4px;">
            {status}
        </div>
        <div style="display: flex; align-items: baseline; justify-content: space-between;">
            <div style="font-size: 36px; font-weight: 900; color: #212121; line-height: 1;">
                {start_date_str}
            </div>
            <div style="font-size: 16px; font-weight: bold; color: #555;">
                {days_held}日目
            </div>
        </div>
        <div style="text-align: right; font-size: 11px; color: #999; margin-top: 6px; font-family: monospace;">
            SN: {serial}
        </div>
    </div>
    """

# --- メイン処理 ---
def main():
    st.set_page_config(page_title="Battery Manager", page_icon="⚡", layout="wide")
    
    # CSS: スライダー調整
    st.markdown("""
        <style>
        .stSlider { padding-top: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    today = get_today_jst()

    # セッション
    if 'parsed_data' not in st.session_state:
        st.session_state['parsed_data'] = None
    if 'search_sn' not in st.session_state:
        st.session_state['search_sn'] = ""

    # データ読み込み
    df = get_data()
    hist_df = get_history()

    # 集計
    week_earnings = 0
    week_count = 0
    total_earnings = 0
    
    if not hist_df.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        weekly_df = hist_df[hist_df['補充日'] >= start_of_week]
        real_jobs_df = weekly_df[~weekly_df['シリアルナンバー'].isin(["手動修正", "過去分", "調整"])]
        week_count = len(real_jobs_df)
        week_earnings = weekly_df['確定報酬額'].sum()
        total_earnings = hist_df['確定報酬額'].sum()

    current_bonus = get_vol_bonus(week_count)

    # --- タブ ---
    tab_home, tab_search, tab_inventory, tab_history = st.tabs(["🏠 ホーム", "🔍 個別検索", "📦 在庫", "💰 収益"])

    # 🏠 ホーム
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
        job_mode = st.radio("作業モード", ["取出 (在庫登録)", "補充 (報酬確定)"], horizontal=True)

        if job_mode == "取出 (在庫登録)":
            st.caption("「バッテリー管理」画面のリスト全体をペースト")
            default_date = st.date_input("基準日 (読取不可時)", value=today)
            input_text = st.text_area("テキスト貼付", height=150, placeholder="ここにペースト...")
            
            if st.button("読込 (内容確認)", type="secondary", icon=":material/search:"):
                if input_text:
                    parsed = extract_serials_with_date(input_text, default_date)
                    if parsed:
                        st.session_state['parsed_data'] = parsed
                        st.rerun()
                    else:
                        st.warning("シリアルナンバーが見つかりませんでした")

            if st.session_state['parsed_data']:
                st.divider()
                st.markdown("##### 以下の内容で登録しますか？")
                preview_df = pd.DataFrame(st.session_state['parsed_data'], columns=["シリアルナンバー", "取得日"])
                st.dataframe(preview_df, hide_index=True, use_container_width=True)
                
                col_reg, col_cancel = st.columns([1, 1])
                with col_reg:
                    if st.button("登録を実行する", type="primary", use_container_width=True, icon=":material/save:"):
                        with st.spinner('登録中...'):
                            added, skipped = add_data_bulk_with_dates(st.session_state['parsed_data'])
                        if added > 0:
                            msg = f"✅ {added} 件を登録しました"
                            if skipped > 0: msg += f" (重複スキップ: {skipped}件)"
                            st.success(msg)
                        else:
                            st.warning(f"⚠️ 全て重複のためスキップされました (スキップ: {skipped}件)")
                        st.session_state['parsed_data'] = None
                        import time
                        time.sleep(2)
                        st.rerun()
                with col_cancel:
                    if st.button("キャンセル", use_container_width=True):
                        st.session_state['parsed_data'] = None
                        st.rerun()

        elif job_mode == "補充 (報酬確定)":
            st.caption("補充したバッテリー番号リストをペースト")
            
            col_date, col_area = st.columns([1, 1])
            with col_date:
                target_date = st.date_input("補充日", value=today)
            with col_area:
                default_index = ZONE_OPTIONS.index("D: その他 (船橋など)")
                selected_zone_name = st.selectbox("エリア選択", ZONE_OPTIONS, index=default_index)

            input_text = st.text_area("テキスト貼付", height=100, placeholder="ここにペースト...")
            
            if input_text:
                extracted = extract_serials_only(input_text)
                if extracted:
                    st.info(f"{len(extracted)} 件を検出しました")
                    
                    base_price = ZONES[selected_zone_name]
                    est_bonus = get_vol_bonus(week_count + len(extracted))
                    est_total_price = base_price + est_bonus
                    
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
        
        # --- おすすめリスト ---
        col_title, col_slider = st.columns([2, 1])
        with col_title:
            st.subheader("ピックアップ推奨")
        with col_slider:
            display_count = st.slider("表示数", 1, 20, 7)

        if not df.empty:
            df_sorted = df.copy() 
            df_sorted['days_held'] = df_sorted['保有開始日'].apply(lambda x: (today - x).days)
            df_sorted['penalty_left'] = PENALTY_LIMIT_DAYS - df_sorted['days_held']
            
            def get_rank(r):
                if r['penalty_left'] <= 5: return 1 # 要返却
                elif r['days_held'] <= 3: return 2  # Bonus
                return 3 # 通常
            
            df_sorted['rank'] = df_sorted.apply(get_rank, axis=1)
            # ソート: ランク(1->2->3) > 日数(多い順＝古い順)
            df_sorted = df_sorted.sort_values(['rank', 'days_held'], ascending=[True, False])
            
            top_n = df_sorted.head(display_count)
            
            if not top_n.empty:
                # 4つずつ表示（スマホ対応）
                for i in range(0, len(top_n), 4):
                    chunk = top_n.iloc[i:i+4]
                    cols = st.columns(4)
                    for idx, (_, row) in enumerate(chunk.iterrows()):
                        with cols[idx]:
                            st.markdown(create_card_html(row, today), unsafe_allow_html=True)
            else:
                st.info("表示対象なし")

    # 🔍 個別検索
    with tab_search:
        st.markdown("### 🔢 個別バッテリー検索")
        search_num = st.number_input(
            "シリアルナンバー (下4桁)", 
            min_value=0, value=0, step=1, format="%d",
            help="タップするとスマホのテンキーが開きます"
        )
        
        if search_num > 0 and not df.empty:
            search_term = str(int(search_num)) 
            hits = df[df['シリアルナンバー'].str.endswith(search_term)]
            st.divider()
            if not hits.empty:
                st.success(f"{len(hits)} 件ヒット")
                for _, row in hits.iterrows():
                    st.markdown(create_card_html(row, today), unsafe_allow_html=True)
            else:
                if len(search_term) >= 4:
                    st.warning("⚠️ 在庫なし")
                    if not hist_df.empty:
                        hist_hits = hist_df[hist_df['シリアルナンバー'].str.endswith(search_term)]
                        if not hist_hits.empty:
                            last_rec = hist_hits.iloc[0]
                            s_date = last_rec['補充日'].strftime('%Y-%m-%d')
                            st.info(f"💡 履歴あり: {s_date} に補充済み")
        else:
            st.info("👆 ボックスをタップして番号を入力してください")

    # 📦 在庫
    with tab_inventory:
        st.subheader("📦 在庫詳細")
        if not df.empty:
            st.metric("現在の在庫総数", f"{len(df)} 本")
            st.divider()

            st.markdown("##### 📅 取得日別の本数")
            date_counts = df['保有開始日'].value_counts().sort_index(ascending=False)
            date_summary = pd.DataFrame({'取得日': date_counts.index, '本数': date_counts.values})
            date_summary['取得日'] = date_summary['取得日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(date_summary, hide_index=True, use_container_width=True)
            st.divider()

            st.markdown("##### 全リスト")
            df_disp = df.copy()
            df_disp['days_held'] = df_disp['保有開始日'].apply(lambda x: (today - x).days)
            def get_rank_simple(r):
                if (PENALTY_LIMIT_DAYS - r['days_held']) <= 5: return 1
                elif r['days_held'] <= 3: return 2
                return 3
            df_disp['rank'] = df_disp.apply(get_rank_simple, axis=1)
            df_disp = df_disp.sort_values(['rank', 'days_held'], ascending=[True, False])
            df_disp['保有開始日'] = df_disp['保有開始日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df_disp = df_disp.rename(columns={'days_held': '経過日数'})
            st.dataframe(df_disp[['シリアルナンバー', '保有開始日', '経過日数']], use_container_width=True, hide_index=True)
        else:
            st.info("在庫はありません")

    # 💰 収益
    with tab_history:
        st.markdown("### 📊 収益レポート")
        col_main, col_sub = st.columns([3, 1])
        with col_main:
            st.metric("🔥 今週の確定報酬", f"¥ {week_earnings:,}")
        with col_sub:
            st.metric("積算 (全期間)", f"¥ {total_earnings:,}")
        st.divider()

        if not hist_df.empty:
            st.markdown("#### 日別推移")
            chart_df = hist_df.groupby('補充日')['確定報酬額'].sum().reset_index()
            chart_df.columns = ['日付', '金額']
            
            chart = alt.Chart(chart_df).mark_bar(color='#29b6f6').encode(
                x=alt.X('日付:T', axis=alt.Axis(format='%m/%d', title='日付', labelAngle=-45)),
                y=alt.Y('金額:Q', axis=alt.Axis(title='金額(円)')),
                tooltip=[
                    alt.Tooltip('日付:T', title='日付', format='%Y-%m-%d'), 
                    alt.Tooltip('金額:Q', title='報酬', format=',')
                ]
            ).interactive()
            st.altair_chart(chart, use_container_width=True)

        st.divider()

        with st.expander("🛠 訂正・過去分登録・調整"):
            st.info("通常は自動計算されます。金額が合わない時の修正や、過去データを登録する時のみ使用してください。")
            adjust_type = st.radio("種別", ["訂正・調整 (+/-)", "過去分 (初期登録)"], horizontal=True)
            with st.form("manual_history_form"):
                col_d, col_a = st.columns([1, 1])
                m_date = col_d.date_input("日付", value=today)
                if adjust_type == "訂正・調整 (+/-)":
                    m_amount = col_a.number_input("調整額 (円)", value=0, step=10, help="足りない場合はプラス、引きたい場合はマイナスを入力")
                    m_memo = st.text_input("理由", placeholder="例: 70円計算だが実際は80円だったため+10円")
                    category = "手動修正"
                else:
                    m_amount = col_a.number_input("売上額 (円)", value=0, step=100)
                    m_memo = st.text_input("備考", placeholder="例: アプリ導入前の12月分合算")
                    category = "過去分"
                
                submitted = st.form_submit_button("履歴に追加", type="primary")
                if submitted:
                    if m_amount != 0:
                        with st.spinner("処理中..."):
                            add_manual_history(m_date, m_amount, m_memo, category)
                        st.success("履歴に追加しました")
                        import time
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("金額を入力してください")

        st.markdown("#### 履歴一覧")
        if not hist_df.empty:
            hist_disp = hist_df.sort_values('補充日', ascending=False).copy()
            hist_disp['補充日'] = hist_disp['補充日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(hist_disp, use_container_width=True)

if __name__ == '__main__':
    main()
