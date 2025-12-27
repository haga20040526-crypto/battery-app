import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import altair as alt
import textwrap
import uuid

# --- 定数・設定 ---
PENALTY_LIMIT_DAYS = 28
NEW_SHEET_NAME = 'database' 
EXPECTED_HEADERS = ['シリアルナンバー', 'ステータス', '保有開始日', '完了日', 'エリア', '金額', '備考']

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
        st.error("システムエラー: Secretsの設定が見つかりません。")
        return None
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

def get_today_jst():
    now = datetime.datetime.now() + datetime.timedelta(hours=9)
    return now.date()

def sanitize_for_json(val):
    if pd.isna(val): return ""
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.strftime('%Y-%m-%d')
    if hasattr(val, 'item'): return val.item()
    return str(val)

# --- テキスト解析 ---
def extract_serials_with_date(text, default_date):
    results = []
    default_date_str = default_date.strftime('%Y-%m-%d')
    if text:
        text = text.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    else: return []
    
    date_pattern = re.compile(r'(\d{4})[-/.](\d{2})[-/.](\d{2})')
    serial_pattern = re.compile(r'\b(\d{8})\b')

    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    for i, line in enumerate(lines):
        serials_in_line = serial_pattern.findall(line)
        if not serials_in_line: continue
        
        search_window = lines[i : min(len(lines), i+4)]
        found_date = default_date_str
        for check_line in search_window:
            d_match = date_pattern.search(check_line)
            if d_match:
                found_date = f"{d_match.group(1)}-{d_match.group(2)}-{d_match.group(3)}"
                break
        
        for s in serials_in_line:
            results.append((s, found_date))
            
    if not results:
        all_serials = serial_pattern.findall(text)
        all_dates = date_pattern.findall(text)
        if all_serials:
            backup_date = f"{all_dates[0][0]}-{all_dates[0][1]}-{all_dates[0][2]}" if all_dates else default_date_str
            for s in all_serials: results.append((s, backup_date))

    unique_map = {r[0]: r[1] for r in results}
    return list(unique_map.items())

def extract_serials_only(text):
    if not text: return []
    text = text.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    return list(set(re.findall(r'\b\d{8}\b', text)))

# --- データ取得 ---
def get_database():
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        try:
            sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
        except:
            wb = client.open('battery_db')
            sheet = wb.add_worksheet(title=NEW_SHEET_NAME, rows=1000, cols=10)
            sheet.append_row(EXPECTED_HEADERS)

        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        if df.empty: return pd.DataFrame(columns=EXPECTED_HEADERS)
        
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        if 'ステータス' in df.columns:
            df['ステータス'] = df['ステータス'].astype(str).str.strip()
        else:
            sheet.insert_row(EXPECTED_HEADERS, index=1)
            return pd.DataFrame(columns=EXPECTED_HEADERS)

        df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0).astype(int)
        for col in ['保有開始日', '完了日']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        return df
    except: return pd.DataFrame()

def get_active_inventory(df_all):
    if df_all.empty or 'ステータス' not in df_all.columns: return pd.DataFrame()
    df = df_all[df_all['ステータス'] == '在庫'].copy()
    if not df.empty:
        df = df.sort_values(by=['保有開始日'], ascending=[True])
        return df
    return df

def get_vol_bonus(count):
    if count >= 150: return 20
    elif count >= 100: return 15
    elif count >= 50: return 10
    elif count >= 20: return 5
    else: return 0

# --- 書き込み・計算ロジック ---

def register_new_inventory(data_list):
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    df = pd.DataFrame(all_records)
    
    current_active_serials = set()
    if not df.empty and 'ステータス' in df.columns:
        active_df = df[df['ステータス'].astype(str).str.strip() == '在庫']
        current_active_serials = set(active_df['シリアルナンバー'].astype(str).tolist())
    
    headers = sheet.row_values(1)
    if not headers: sheet.append_row(EXPECTED_HEADERS)

    rows = []
    skipped = 0
    for s, d in data_list:
        s_str = str(s)
        if s_str in current_active_serials:
            skipped += 1
            continue
        row = [sanitize_for_json(s_str), "在庫", sanitize_for_json(d), "", "", "", ""]
        rows.append(row)
    
    if rows:
        try: sheet.append_rows(rows)
        except: return 0, 0
    return len(rows), skipped

def register_past_bulk(date_obj, count, total_amount, zone, memo=""):
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    headers = sheet.row_values(1)
    if not headers: sheet.append_row(EXPECTED_HEADERS)
    if count <= 0: return 0
    
    base_amount = total_amount // count
    remainder = total_amount % count
    date_str = date_obj.strftime('%Y-%m-%d')
    rows = []
    for i in range(count):
        dummy_sn = f"OLD-{date_str.replace('-','')}-{uuid.uuid4().hex[:6]}"
        amount = base_amount + (1 if i < remainder else 0)
        row = [dummy_sn, "補充済", "", date_str, zone, amount, memo]
        rows.append(row)
    if rows: sheet.append_rows(rows)
    return len(rows)

def recalc_weekly_revenue(sheet, today_date):
    all_records = sheet.get_all_records()
    headers = sheet.row_values(1)
    try: col_price = headers.index('金額') + 1
    except: return 0

    start_of_week = today_date - datetime.timedelta(days=today_date.weekday())
    end_of_week = start_of_week + datetime.timedelta(days=6)

    weekly_indices = []
    for i, row in enumerate(all_records):
        st_val = str(row.get('ステータス', '')).strip()
        comp_date_str = str(row.get('完了日', ''))
        sn = str(row.get('シリアルナンバー', ''))
        memo = str(row.get('備考', ''))
        
        if st_val == '補充済' and comp_date_str and 'ボーナス' not in memo:
            try:
                comp_date = datetime.datetime.strptime(comp_date_str, '%Y-%m-%d').date()
                if start_of_week <= comp_date <= end_of_week:
                    weekly_indices.append(i)
            except: pass

    week_count = len(weekly_indices)
    current_bonus = get_vol_bonus(week_count)
    
    cells_to_update = []
    for idx in weekly_indices:
        row = all_records[idx]
        zone_name = str(row.get('エリア', ''))
        base_price = ZONES.get(zone_name, 70)
        start_d_str = str(row.get('保有開始日', ''))
        end_d_str = str(row.get('完了日', ''))
        early_bonus = 0
        try:
            s_date = datetime.datetime.strptime(start_d_str, '%Y-%m-%d').date()
            e_date = datetime.datetime.strptime(end_d_str, '%Y-%m-%d').date()
            if (e_date - s_date).days <= 3: early_bonus = 10
        except: pass
        
        new_total_price = base_price + current_bonus + early_bonus
        if row.get('金額', 0) != new_total_price:
            cells_to_update.append(gspread.Cell(idx + 2, col_price, new_total_price))

    if cells_to_update:
        try: sheet.update_cells(cells_to_update)
        except: pass
    return len(cells_to_update)

def update_status_bulk(target_serials, new_status, complete_date=None, zone="", price=0, memo=""):
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    headers = sheet.row_values(1)
    try:
        col_status = headers.index('ステータス') + 1
        col_end = headers.index('完了日') + 1
        col_zone = headers.index('エリア') + 1
        col_price = headers.index('金額') + 1
        col_memo = headers.index('備考') + 1
    except: return 0

    cells = []
    updated = 0
    target_set = set(str(s) for s in target_serials)
    comp_str = sanitize_for_json(complete_date)
    safe_price = int(price)

    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = str(row.get('ステータス', '')).strip()
        if st_val == '在庫' and s in target_set:
            r = i + 2
            cells.append(gspread.Cell(r, col_status, new_status))
            cells.append(gspread.Cell(r, col_end, comp_str))
            cells.append(gspread.Cell(r, col_zone, zone))
            cells.append(gspread.Cell(r, col_price, safe_price))
            if memo: cells.append(gspread.Cell(r, col_memo, memo))
            updated += 1
            
    if cells:
        try: sheet.update_cells(cells)
        except: return 0
    
    if updated > 0 and new_status == '補充済' and complete_date:
        recalc_weekly_revenue(sheet, complete_date)
    return updated

def update_dates_bulk(updates_list):
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    headers = sheet.row_values(1)
    if '保有開始日' not in headers: return 0
    col_start = headers.index('保有開始日') + 1
    cells = []
    updates_map = {str(s): sanitize_for_json(d) for s, d in updates_list}
    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = str(row.get('ステータス', '')).strip()
        if st_val == '在庫' and s in updates_map:
            r = i + 2
            cells.append(gspread.Cell(r, col_start, updates_map[s]))
    if cells:
        try: sheet.update_cells(cells)
        except: return 0
    return len(cells)

# --- UIパーツ ---
def create_card(row, today):
    start_date = row.get('保有開始日')
    status = str(row.get('ステータス', '')).strip()
    sn = row['シリアルナンバー']
    last4 = sn[-4:]
    
    if pd.isnull(start_date):
        s_str, days = "-", 0
    else:
        s_str = start_date.strftime('%m/%d')
        days = (today - start_date).days
    
    if status == '補充済':
        c, bg, st_t, bd = "#1565c0", "#e3f2fd", "✅ 完了", "#2196f3"
        date_label = f"完了: {s_str}"
        main_text = "補充済"
    elif status == '不明' or '削除' in status or 'エラー' in status:
        c, bg, st_t, bd = "#757575", "#f5f5f5", "🚫 除外", "#bdbdbd"
        date_label = "-"
        main_text = "除外済"
    else:
        p_days = PENALTY_LIMIT_DAYS - days
        if p_days <= 5: 
            c, bg, st_t, bd = "#c62828", "#ffebee", f"🔥 残{p_days}日", "#ef5350"
        elif days <= 3: 
            c, bg, st_t, bd = "#2e7d32", "#e8f5e9", "💎 Bonus対象", "#66bb6a"
        else: 
            c, bg, st_t, bd = "#424242", "#ffffff", "🐢 通常", "#bdbdbd"
        date_label = f"取得: {s_str}"
        main_text = last4

    return textwrap.dedent(f"""
    <div style="background:{bg}; border-radius:8px; border-left:6px solid {bd}; padding:10px; margin-bottom:8px; box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <div style="display:flex; justify-content:space-between; font-size:11px; font-weight:bold; color:{c};">
            <div>{st_t}</div><div>{date_label}</div>
        </div>
        <div style="font-size:28px; font-weight:900; color:#212121; margin-top:2px; letter-spacing:1px;">{main_text}</div>
        <div style="text-align:right; font-size:9px; color:#999; font-family:monospace;">{sn}</div>
    </div>
    """)

# --- メイン ---
def main():
    st.set_page_config(page_title="Battery Manager V26", page_icon="⚡", layout="wide")
    
    # ▼ ヘッダーデザイン ▼
    st.markdown("""
        <div style='display: flex; align-items: center; border-bottom: 2px solid #ff7043; padding-bottom: 10px; margin-bottom: 20px;'>
            <div style='font-size: 40px; margin-right: 15px;'>⚡</div>
            <div>
                <h1 style='margin: 0; padding: 0; font-size: 32px; color: #333; font-family: sans-serif; letter-spacing: -1px;'>Battery Manager</h1>
                <div style='font-size: 14px; color: #757575;'>Profit Optimization & Inventory Control <span style='color: #ff7043; font-weight: bold; margin-left:8px;'>V26</span></div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    # ▲ ここまで ▲

    st.markdown("<style>.stSlider{padding-top:1rem;}</style>", unsafe_allow_html=True)
    today = get_today_jst()

    if 'stocktake_buffer' not in st.session_state: st.session_state['stocktake_buffer'] = []
    if 'parsed_data' not in st.session_state: st.session_state['parsed_data'] = None

    df_all = get_database()
    
    if not df_all.empty and 'ステータス' in df_all.columns:
        df_valid = df_all[~df_all['ステータス'].str.contains('削除', na=False)]
        df_inv = get_active_inventory(df_valid)
        df_hist = df_valid[df_valid['ステータス'] != '在庫'].copy()
    else:
        df_inv = pd.DataFrame()
        df_hist = pd.DataFrame()

    week_earnings = 0
    week_count = 0
    next_bonus_at = 20
    
    if not df_hist.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        df_hist['comp_date'] = pd.to_datetime(df_hist['完了日'], errors='coerce')
        
        w_df = df_hist[
            (df_hist['comp_date'].dt.date >= start_of_week) & 
            (df_hist['ステータス'] == '補充済')
        ].copy()
        
        count_mask = w_df.apply(lambda x: 'ボーナス' not in str(x['備考']), axis=1)
        week_count = len(w_df[count_mask])
        week_earnings = int(w_df['金額'].sum())
        
        if week_count < 20: next_bonus_at = 20
        elif week_count < 50: next_bonus_at = 50
        elif week_count < 100: next_bonus_at = 100
        elif week_count < 150: next_bonus_at = 150
        else: next_bonus_at = 999

    cur_bonus = get_vol_bonus(week_count)

    if next_bonus_at != 999:
        remain = next_bonus_at - week_count
        st.caption(f"🔥 今週の目標: {next_bonus_at}本まで あと**{remain}本**")
        st.progress(min(week_count / next_bonus_at, 1.0))
    else:
        st.success(f"👑 MAXランク到達！ (+{cur_bonus}円)")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏠 ホーム", "🔍 検索", "📦 在庫", "💰 収益", "📝 棚卸"])

    # 1. ホーム
    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬", f"¥ {week_earnings:,}")
        c2.metric("本数", f"{week_count} 本")
        c3.metric("現在ボナ", f"+{cur_bonus}円/本")
        st.divider()

        mode = st.radio("作業モード", ["取出 (登録)", "補充 (確定)"], horizontal=True)
        
        if mode == "取出 (登録)":
            txt = st.text_area("リスト貼付", height=100, placeholder="保有中リストをここにペースト")
            date_in = st.date_input("基準日 (読取不可時)", value=today)
            if st.button("読込", icon=":material/search:"):
                if txt:
                    parsed = extract_serials_with_date(txt, date_in)
                    st.session_state['parsed_data'] = parsed
                    if parsed: st.success(f"{len(parsed)} 件 読込")
                    else: st.warning("番号が見つかりませんでした")
            
            if st.session_state['parsed_data']:
                st.dataframe(pd.DataFrame(st.session_state['parsed_data'], columns=["SN","日付"]), hide_index=True)
                if st.button("登録実行", type="primary", use_container_width=True):
                    cnt, skip = register_new_inventory(st.session_state['parsed_data'])
                    msg = f"✅ {cnt}件 登録"
                    if skip > 0: msg += f" (手元重複 {skip}件 スキップ)"
                    st.success(msg)
                    st.session_state['parsed_data'] = None
                    import time
                    time.sleep(1)
                    st.rerun()

        else: 
            col_d, col_z = st.columns([1,1])
            date_done = col_d.date_input("補充日", value=today)
            zone = col_z.selectbox("エリア", ZONE_OPTIONS)
            txt = st.text_area("リスト貼付", height=100, placeholder="完了画面をここにペースト")
            if txt:
                sns = extract_serials_only(txt)
                if sns:
                    st.info(f"{len(sns)}件 検出")
                    if st.button("補充確定", type="primary", use_container_width=True):
                        base = ZONES[zone]
                        cnt = update_status_bulk(sns, "補充済", date_done, zone, base)
                        st.success(f"{cnt}件 更新しました")
                        import time
                        time.sleep(1)
                        st.rerun()

        st.divider()
        st.markdown("##### 📌 ピックアップ (優先順)")
        col_sl, _ = st.columns([1,2])
        with col_sl:
            disp_count = st.slider("表示数", 4, 40, 8, step=4)

        if not df_inv.empty:
            df_disp = df_inv.copy()
            def get_priority(row):
                days = (today - row['保有開始日']).days
                if days >= (PENALTY_LIMIT_DAYS - 5): return 1
                if days <= 3: return 2
                return 3
            df_disp['rank'] = df_disp.apply(get_priority, axis=1)
            df_disp = df_disp.sort_values(by=['rank', '保有開始日'], ascending=[True, True])
            
            top_n = df_disp.head(disp_count)
            for i in range(0, len(top_n), 4):
                cols = st.columns(4)
                chunk = top_n.iloc[i:i+4]
                for j, (_, row) in enumerate(chunk.iterrows()):
                    with cols[j]:
                        st.markdown(create_card(row, today), unsafe_allow_html=True)
        else: st.info("現在、在庫はありません")

    # 2. 検索 (V26: 日付絞り込み追加)
    with tab2:
        # 日付リスト作成 (在庫のみ)
        date_options = ["指定なし"]
        date_map = {}
        if not df_inv.empty:
            unique_dates = sorted(df_inv['保有開始日'].unique(), reverse=True) # 新しい順
            for d in unique_dates:
                if pd.notnull(d):
                    label = d.strftime('%m/%d')
                    date_options.append(label)
                    date_map[label] = d

        c_s1, c_s2 = st.columns(2)
        with c_s1:
            sel_date = st.selectbox("保有開始日 (在庫のみ)", date_options)
        with c_s2:
            sn_in = st.number_input("SN下4桁", 0, 9999, 0)

        # 検索ロジック
        results = pd.DataFrame()
        
        # 1. 日付指定がある場合 -> 在庫から検索
        if sel_date != "指定なし":
            target_date = date_map[sel_date]
            results = df_inv[df_inv['保有開始日'] == target_date].copy()
            if sn_in > 0: # 番号もあればさらに絞り込み
                results = results[results['シリアルナンバー'].str.endswith(str(sn_in))]
            
            if not results.empty:
                st.success(f"{len(results)}件 (保有日: {sel_date})")
                for _, row in results.iterrows():
                    st.markdown(create_card(row, today), unsafe_allow_html=True)
            else:
                st.warning("該当なし")

        # 2. 日付指定なし & 番号あり -> 全期間から検索 (既存機能)
        elif sn_in > 0:
            if not df_all.empty:
                results = df_all[df_all['シリアルナンバー'].str.endswith(str(sn_in))]
                if not results.empty:
                    st.success(f"{len(results)}件 ヒット (全期間)")
                    for _, row in results.iterrows():
                        st.markdown(create_card(row, today), unsafe_allow_html=True)
                else:
                    st.warning("なし")
        
        else:
            st.info("条件を指定してください")

    # 3. 在庫
    with tab3:
        st.metric("在庫数", f"{len(df_inv)}")
        if not df_inv.empty:
            st.dataframe(df_inv[['保有開始日', 'シリアルナンバー']], use_container_width=True)

    # 4. 収益
    with tab4:
        st.metric("今週", f"¥{week_earnings:,}")
        
        with st.expander("➕ 過去データの登録"):
            with st.form("manual_past_reg"):
                c1, c2 = st.columns(2)
                p_date = c1.date_input("完了日")
                p_count = c2.number_input("数量", min_value=1, value=1)
                p_amount = c1.number_input("合計金額", step=10)
                p_zone = c2.selectbox("エリア", ZONE_OPTIONS)
                p_memo = st.text_input("備考", placeholder="ボーナスなど")
                if st.form_submit_button("登録"):
                    reg_cnt = register_past_bulk(p_date, p_count, p_amount, p_zone, p_memo)
                    st.success(f"{reg_cnt}行 登録完了")
                    import time
                    time.sleep(1)
                    st.rerun()

        if not df_hist.empty:
            df_wk = df_hist[df_hist['ステータス'] == '補充済'].copy()
            if not df_wk.empty:
                df_wk['date'] = pd.to_datetime(df_wk['完了日'])
                df_wk['week_start'] = df_wk['date'].apply(lambda x: x - datetime.timedelta(days=x.weekday()))
                df_wk['is_battery'] = df_wk['備考'].apply(lambda x: 0 if 'ボーナス' in str(x) else 1)
                
                weekly_agg = df_wk.groupby('week_start').agg(
                    total_amount=('金額', 'sum'),
                    count=('is_battery', 'sum')
                ).reset_index().sort_values('week_start', ascending=False)
                weekly_agg['Label'] = weekly_agg['week_start'].dt.strftime('%Y/%m/%d') + " 週"

                st.divider()
                st.subheader("📈 週次比較")
                
                chart_data = weekly_agg.sort_values('week_start', ascending=True)
                base = alt.Chart(chart_data).encode(x=alt.X('Label', sort=None, title='週'))
                bar = base.mark_bar(color='#4fc3f7').encode(
                    y=alt.Y('total_amount', title='金額', axis=alt.Axis(titleColor='#0277bd')),
                    tooltip=['Label', 'total_amount', 'count']
                )
                line = base.mark_line(color='#ff7043', strokeWidth=3).encode(
                    y=alt.Y('count', title='本数', axis=alt.Axis(titleColor='#ff7043'))
                )
                points = base.mark_circle(color='#ff7043', size=60).encode(
                    y=alt.Y('count', axis=None)
                )
                st.altair_chart(alt.layer(bar, line + points).resolve_scale(y='independent').properties(height=300), use_container_width=True)
                
                st.markdown("##### 📊 週間集計")
                display_df = weekly_agg[['Label', 'total_amount', 'count']].rename(
                    columns={'Label': '週 (月曜開始)', 'total_amount': '合計金額 (円)', 'count': '本数 (本)'}
                )
                st.dataframe(display_df, hide_index=True, use_container_width=True)
                
                with st.expander("詳細リスト (全履歴)"):
                    display_cols = ['完了日', '金額', 'シリアルナンバー', 'エリア', '備考']
                    st.dataframe(df_wk.sort_values('date', ascending=False)[display_cols], use_container_width=True)

    # 5. 棚卸
    with tab5:
        st.subheader("在庫棚卸し")
        st.caption("SpotJobsの「保有リスト(全量)」を貼り付けると、新規追加と消失検知が同時に行えます。")
        
        cur = st.session_state['stocktake_buffer']
        c1, c2 = st.columns([1,1])
        with c1:
            txt_stock = st.text_area("全リスト貼付")
            if st.button("リストを読込"):
                if txt_stock:
                    add = extract_serials_with_date(txt_stock, today)
                    st.session_state['stocktake_buffer'] = add
                    st.rerun()
            if st.button("クリア"):
                st.session_state['stocktake_buffer'] = []
                st.rerun()
        with c2:
            st.caption(f"読込: {len(cur)}件")
            if cur: st.dataframe(pd.DataFrame(cur, columns=["SN","日付"]), height=150, hide_index=True)

        st.divider()
        
        if cur:
            s_map = {s:d for s,d in cur}
            input_set = set(s_map.keys())
            
            db_map = {}
            if not df_inv.empty:
                db_map = dict(zip(df_inv['シリアルナンバー'], df_inv['保有開始日']))
            db_set = set(db_map.keys())
            
            missing_db = []
            for s, d in s_map.items():
                if s not in db_map: missing_db.append((s, d))
            
            ghosts = list(db_set - input_set)
            
            c_act1, c_act2 = st.columns(2)
            
            with c_act1:
                st.markdown(f"**① 新規在庫: {len(missing_db)}件**")
                if missing_db:
                    if st.button("新規分を登録", type="primary"):
                        cnt, _ = register_new_inventory(missing_db)
                        st.success(f"{cnt}件 登録しました")
                        import time
                        time.sleep(1)
                        st.rerun()
                else: st.info("新規なし")
                
            with c_act2:
                st.markdown(f"**② 消失・エラー検知: {len(ghosts)}件**")
                if ghosts:
                    st.warning("アプリにはあるが、リストにない在庫です。")
                    with st.expander("詳細を確認"):
                        st.write(ghosts)
                    if st.button("一括「補充エラー」にする"):
                        cnt = update_status_bulk(ghosts, "補充エラー", today, "", 0, "棚卸検知")
                        st.success(f"{cnt}件 を在庫から除外しました")
                        import time
                        time.sleep(1)
                        st.rerun()
                else: st.success("差異なし")

if __name__ == '__main__':
    main()
