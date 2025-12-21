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
                # 日付形式 (YYYY-MM-DD or YYYY/MM/DD) を探す
                d_match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2})', block)
                if d_match:
                    d_str = d_match.group(1).replace('/', '-')
                    results.append((serial, d_str))
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

# --- ★カスタムソート: 日付 > 末尾の数字順 ---
def sort_batteries(df):
    if df.empty:
        return df
    # シリアルナンバーを逆順にした文字列を作成（末尾比較用）
    df['rev_serial'] = df['シリアルナンバー'].apply(lambda x: x[::-1])
    # 日付(昇順) -> 逆シリアル(昇順) でソート
    df_sorted = df.sort_values(by=['保有開始日', 'rev_serial'], ascending=[True, True])
    df_sorted = df_sorted.drop(columns=['rev_serial'])
    return df_sorted

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
        
        # 取得時点でカスタムソートを適用
        return sort_batteries(df)
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
        
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
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

    df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
    
    rows_to_delete = []
    history_rows = []
    
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

def delete_data_by_serial(serial):
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    all_records = sheet.get_all_records()
    df = pd.DataFrame(all_records)
    if df.empty: return False
    df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
    target = df[df['シリアルナンバー'] == str(serial)]
    if not target.empty:
        row_idx = target.index[0] + 2
        sheet.delete_rows(row_idx)
        return True
    return False

# --- 棚卸し用処理 ---
def archive_missing_items(serials, today_date):
    """手元にない在庫を削除し、履歴に「棚卸」として保存"""
    client = get_connection()
    db_sheet = client.open(SHEET_NAME).sheet1
    hist_sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
    
    all_records = db_sheet.get_all_records()
    df = pd.DataFrame(all_records)
    df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
    
    rows_to_delete = []
    history_rows = []
    date_str = today_date.strftime('%Y-%m-%d')

    for s in serials:
        target = df[df['シリアルナンバー'] == str(s)]
        if not target.empty:
            start_date = pd.to_datetime(target.iloc[0]['保有開始日']).date()
            row_idx = target.index[0] + 2
            rows_to_delete.append(row_idx)
            history_rows.append([
                str(s), str(start_date), date_str, "棚卸", 0, "棚卸削除(手元なし)"
            ])

    if history_rows:
        hist_sheet.append_rows(history_rows)

    rows_to_delete.sort(reverse=True)
    for r in rows_to_delete:
        db_sheet.delete_rows(r)
    return len(rows_to_delete)

def update_inventory_dates(updates_list):
    """日付ズレを修正"""
    client = get_connection()
    sheet = client.open(SHEET_NAME).sheet1
    all_records = sheet.get_all_records()
    
    serial_to_row = {str(row['シリアルナンバー']): i + 2 for i, row in enumerate(all_records)}
    update_count = 0
    for s, new_date in updates_list:
        if str(s) in serial_to_row:
            row_idx = serial_to_row[str(s)]
            sheet.update_cell(row_idx, 2, new_date)
            update_count += 1
    return update_count

def add_manual_history(date_obj, amount, memo, category):
    client = get_connection()
    hist_sheet = client.open(SHEET_NAME).worksheet(HISTORY_SHEET_NAME)
    date_str = date_obj.strftime('%Y-%m-%d')
    row = [category, "-", date_str, "-", amount, memo]
    hist_sheet.append_row(row)

# --- カード表示: 在庫リスト用 (SN重視) ---
def create_inventory_card_html(row, today):
    p_days = PENALTY_LIMIT_DAYS - (today - row['保有開始日']).days
    days_held = (today - row['保有開始日']).days
    serial = row['シリアルナンバー']
    last4 = serial[-4:] if len(serial) >= 4 else serial
    start_date_str = row['保有開始日'].strftime('%m/%d')
    
    if p_days <= 5: priority = 1
    elif days_held <= 3: priority = 2
    else: priority = 3

    if priority == 1:
        border, text_c, status = "#e57373", "#c62828", f"🔥 要返却 (残{p_days}日)"
        bg_c = "#fff5f5"
    elif priority == 2:
        border, text_c, status = "#81c784", "#2e7d32", "💎 Bonus"
        bg_c = "#f1f8e9"
    else:
        border, text_c, status = "#bdbdbd", "#616161", f"🐢 通常 (残{p_days}日)"
        bg_c = "#ffffff"
    
    return f"""
    <div style="background-color: {bg_c}; border-radius: 8px; border-left: 8px solid {border}; 
        box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 12px; margin-bottom: 12px;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
            <div style="font-size: 12px; font-weight: bold; color: {text_c};">{status}</div>
            <div style="font-size: 12px; font-weight: bold; color: #555;">{start_date_str}〜</div>
        </div>
        <div style="font-size: 34px; font-weight: 900; color: #212121; line-height: 1.1; letter-spacing: 1px;">
            {last4}
        </div>
        <div style="text-align: right; font-size: 10px; color: #999; font-family: monospace;">
            {serial}
        </div>
    </div>
    """

# --- カード表示: 検索用 (シンプル・日付重視) ---
def create_search_card_html(row, today):
    days_held = (today - row['保有開始日']).days
    serial = row['シリアルナンバー']
    start_date_str = row['保有開始日'].strftime('%Y-%m-%d')
    
    return f"""
    <div style="background-color: #ffffff; border-radius: 12px; border: 1px solid #e0e0e0;
        padding: 15px; margin-bottom: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
        
        <div style="font-size: 13px; color: #757575; margin-bottom: 4px;">保管開始日</div>
        <div style="font-size: 42px; font-weight: 900; color: #212121; line-height: 1.1; letter-spacing: 1px;">{start_date_str}</div>
        
        <div style="font-size: 18px; font-weight: bold; color: #424242; margin-top: 8px; background-color: #f5f5f5; display: inline-block; padding: 4px 12px; border-radius: 20px;">
            経過 {days_held}日目
        </div>

        <div style="font-size: 12px; color: #bdbdbd; margin-top: 15px; padding-top: 8px; border-top: 1px solid #f0f0f0; font-family: monospace; text-align: right;">
            SN: {serial}
        </div>
    </div>
    """

# --- メイン処理 ---
def main():
    st.set_page_config(page_title="Battery Manager", page_icon="⚡", layout="wide")
    
    # CSS削除 (デフォルトのレスポンシブ動作に任せる)
    
    today = get_today_jst()

    # セッション
    if 'parsed_data' not in st.session_state:
        st.session_state['parsed_data'] = None
    if 'search_sn' not in st.session_state:
        st.session_state['search_sn'] = ""
    if 'stocktake_buffer' not in st.session_state:
        st.session_state['stocktake_buffer'] = []

    df = get_data() # カスタムソート済み
    hist_df = get_history()

    week_earnings = 0
    week_count = 0
    total_earnings = 0
    
    if not hist_df.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        weekly_df = hist_df[hist_df['補充日'] >= start_of_week]
        real_jobs_df = weekly_df[~weekly_df['シリアルナンバー'].isin(["手動修正", "過去分", "調整", "棚卸"])]
        week_count = len(real_jobs_df)
        week_earnings = weekly_df['確定報酬額'].sum()
        total_earnings = hist_df['確定報酬額'].sum()

    current_bonus = get_vol_bonus(week_count)

    # --- タブ (棚卸しを一番右へ) ---
    tab_home, tab_search, tab_inventory, tab_history, tab_stocktake = st.tabs(["🏠 ホーム", "🔍 個別検索", "📦 在庫", "💰 収益", "📝 棚卸し"])

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
            st.caption("SpotJobsアプリのリストをペーストして登録します。")
            default_date = st.date_input("基準日 (読取不可時)", value=today)
            input_text = st.text_area("テキスト貼付", height=100, placeholder="ここにペースト...")
            
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
                            st.warning(f"⚠️ 全て重複のためスキップされました")
                        st.session_state['parsed_data'] = None
                        import time
                        time.sleep(2)
                        st.rerun()
                with col_cancel:
                    if st.button("キャンセル", use_container_width=True):
                        st.session_state['parsed_data'] = None
                        st.rerun()

        elif job_mode == "補充 (報酬確定)":
            st.caption("補充したバッテリーを在庫から消し、報酬履歴に追加します。")
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
                            st.error("エラー: 在庫から該当番号が見つかりませんでした")
        
        st.divider()
        
        # --- おすすめリスト ---
        col_title, col_slider = st.columns([2, 1])
        with col_title:
            st.subheader("ピックアップ推奨")
        with col_slider:
            display_count = st.slider("表示数", 1, 20, 7)

        if not df.empty:
            # 優先度計算してTopN抽出
            df_rec = df.copy()
            df_rec['days_held'] = df_rec['保有開始日'].apply(lambda x: (today - x).days)
            df_rec['penalty_left'] = PENALTY_LIMIT_DAYS - df_rec['days_held']
            
            def get_rank(r):
                if r['penalty_left'] <= 5: return 1 
                elif r['days_held'] <= 3: return 2
                return 3
            df_rec['rank'] = df_rec.apply(get_rank, axis=1)
            
            # 優先度順に抽出
            df_rec_sorted = df_rec.sort_values(['rank', 'days_held'], ascending=[True, False])
            top_n = df_rec_sorted.head(display_count)
            
            # 抽出後のリストを「現場並び（日付＞末尾）」にソート
            top_n_display = sort_batteries(top_n)
            
            if not top_n_display.empty:
                for i in range(0, len(top_n_display), 4):
                    chunk = top_n_display.iloc[i:i+4]
                    cols = st.columns(4)
                    for idx, (_, row) in enumerate(chunk.iterrows()):
                        with cols[idx]:
                            st.markdown(create_inventory_card_html(row, today), unsafe_allow_html=True)
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
                    st.markdown(create_search_card_html(row, today), unsafe_allow_html=True)
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

            # 削除機能
            with st.expander("🗑️ 在庫から削除 (エラー補充対応)", expanded=False):
                del_serial = st.text_input("削除するシリアルナンバー (8桁)")
                if st.button("削除を実行"):
                    if delete_data_by_serial(del_serial):
                        st.success(f"✅ {del_serial} を削除しました")
                        import time
                        time.sleep(1)
                        st.rerun()
            
            st.divider()
            
            # 日付別集計
            date_counts = df['保有開始日'].value_counts().sort_index(ascending=False)
            date_summary = pd.DataFrame({'取得日': date_counts.index, '本数': date_counts.values})
            date_summary['取得日'] = date_summary['取得日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(date_summary, hide_index=True, use_container_width=True)
            st.divider()

            st.markdown("##### 全リスト (日付順 > 末尾順)")
            df_disp = df.copy()
            df_disp['保有開始日'] = df_disp['保有開始日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(df_disp, use_container_width=True, hide_index=True)
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
            chart_df = hist_df.groupby('補充日')['確定報酬額'].sum().reset_index()
            chart_df.columns = ['日付', '金額']
            chart = alt.Chart(chart_df).mark_bar(color='#29b6f6').encode(
                x=alt.X('日付:T', axis=alt.Axis(format='%m/%d', title='日付', labelAngle=-45)),
                y=alt.Y('金額:Q', axis=alt.Axis(title='金額(円)')),
                tooltip=[alt.Tooltip('日付:T', format='%Y-%m-%d'), alt.Tooltip('金額:Q', format=',')]
            ).interactive()
            st.altair_chart(chart, use_container_width=True)

        with st.expander("🛠 訂正・過去分登録"):
            with st.form("manual_history_form"):
                col_d, col_a = st.columns([1, 1])
                m_date = col_d.date_input("日付", value=today)
                m_amount = col_a.number_input("金額 (円)", step=10)
                m_memo = st.text_input("備考")
                if st.form_submit_button("追加"):
                    add_manual_history(m_date, m_amount, m_memo, "手動")
                    st.rerun()

        st.markdown("#### 履歴一覧")
        if not hist_df.empty:
            hist_disp = hist_df.sort_values('補充日', ascending=False).copy()
            hist_disp['補充日'] = hist_disp['補充日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(hist_disp, use_container_width=True)

    # 📝 棚卸し (新機能)
    with tab_stocktake:
        st.markdown("### 📝 在庫棚卸し")
        st.caption("SpotJobsアプリの保有リストを貼り付けて、ズレを確認します。")
        
        # バッファ表示
        current_buffer = st.session_state['stocktake_buffer']
        st.info(f"現在読み込み済み: {len(current_buffer)} 本")
        
        with st.expander("データの追加読込", expanded=True):
            stock_input = st.text_area("リスト貼り付け (分割可)", height=100)
            c_add, c_clear = st.columns([1, 1])
            with c_add:
                if st.button("リストに追加", type="primary", icon=":material/add:"):
                    if stock_input:
                        new_items = extract_serials_with_date(stock_input, today)
                        if new_items:
                            st.session_state['stocktake_buffer'].extend(new_items)
                            # 重複排除
                            unique_buffer = {}
                            for s, d in st.session_state['stocktake_buffer']:
                                unique_buffer[s] = d
                            st.session_state['stocktake_buffer'] = list(unique_buffer.items())
                            st.rerun()
            with c_clear:
                if st.button("リセット", icon=":material/delete:"):
                    st.session_state['stocktake_buffer'] = []
                    st.rerun()

        st.divider()
        
        if st.button("照合開始", type="primary", use_container_width=True):
            if not df.empty and current_buffer:
                # 照合処理
                stock_map = {s: d for s, d in current_buffer}
                db_map = dict(zip(df['シリアルナンバー'], df['保有開始日']))
                
                def fmt_date(d): return pd.to_datetime(d).strftime('%Y-%m-%d')

                missing_in_db = []
                missing_in_hand = []
                date_mismatch = []
                
                # A Check (未登録 & 日付ズレ)
                for s, d in stock_map.items():
                    if s not in db_map:
                        missing_in_db.append((s, d))
                    else:
                        if fmt_date(db_map[s]) != fmt_date(d):
                            date_mismatch.append((s, fmt_date(d), fmt_date(db_map[s])))
                
                # B Check (手元なし)
                for s in db_map.keys():
                    if s not in stock_map:
                        missing_in_hand.append(s)
                
                if not missing_in_db and not missing_in_hand and not date_mismatch:
                    st.success("🎉 ズレはありません！完璧です！")
                else:
                    if missing_in_db:
                        st.warning(f"🚨 未登録のバッテリー: {len(missing_in_db)} 件")
                        with st.expander("詳細＆登録"):
                            st.dataframe(pd.DataFrame(missing_in_db, columns=["SN", "日付"]), hide_index=True)
                            if st.button("一括登録する"):
                                add_data_bulk_with_dates(missing_in_db)
                                st.success("登録しました")
                                st.rerun()
                    
                    if missing_in_hand:
                        st.error(f"⚠️ 手元に無い (アプリのみ存在): {len(missing_in_hand)} 件")
                        with st.expander("詳細＆削除処理"):
                            st.write(", ".join(missing_in_hand))
                            st.caption("※これらは「棚卸不明」として在庫から消し、履歴に残します。")
                            if st.button("一括処理 (履歴へ移動)"):
                                count = archive_missing_items(missing_in_hand, today)
                                st.success(f"{count} 件を処理しました")
                                st.rerun()

                    if date_mismatch:
                        st.info(f"📅 日付ズレ: {len(date_mismatch)} 件")
                        with st.expander("詳細＆更新"):
                            mismatch_df = pd.DataFrame(date_mismatch, columns=["SN", "正しい日付(手元)", "古い日付(アプリ)"])
                            st.dataframe(mismatch_df, hide_index=True)
                            if st.button("日付を更新する"):
                                updates = [(item[0], item[1]) for item in date_mismatch]
                                cnt = update_inventory_dates(updates)
                                st.success(f"{cnt} 件の日付を更新しました")
                                st.rerun()
            else:
                st.warning("在庫データまたは入力データが空です。")

if __name__ == '__main__':
    main()
