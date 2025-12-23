import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import altair as alt
import textwrap

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
        st.error("Secretsの設定が見つかりません。")
        return None
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

def get_today_jst():
    now = datetime.datetime.now() + datetime.timedelta(hours=9)
    return now.date()

# --- 強力なテキスト解析 ---
def extract_serials_with_date(text, default_date):
    results = []
    default_date_str = default_date.strftime('%Y-%m-%d')
    date_pattern = re.compile(r'(\d{4})[-/.](\d{2})[-/.](\d{2})')
    serial_pattern = re.compile(r'\b(\d{8})\b')

    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    for i, line in enumerate(lines):
        serials_in_line = serial_pattern.findall(line)
        if not serials_in_line: continue
            
        search_window = lines[max(0, i-1) : min(len(lines), i+3)]
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
    return list(set(re.findall(r'\b\d{8}\b', text)))

# --- データ取得 ---
def get_database():
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        try:
            sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            try:
                wb = client.open('battery_db')
                sheet = wb.add_worksheet(title=NEW_SHEET_NAME, rows=1000, cols=10)
                sheet.append_row(EXPECTED_HEADERS)
            except: return pd.DataFrame()

        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        if df.empty: return pd.DataFrame(columns=EXPECTED_HEADERS)
        
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        if 'ステータス' in df.columns:
            df['ステータス'] = df['ステータス'].astype(str).str.strip()
        else:
            sheet.clear()
            sheet.append_row(EXPECTED_HEADERS)
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
        df['rev_serial'] = df['シリアルナンバー'].apply(lambda x: x[::-1])
        df_sorted = df.sort_values(by=['保有開始日', 'rev_serial'], ascending=[True, True])
        return df_sorted.drop(columns=['rev_serial'])
    return df

def get_vol_bonus(count):
    if count >= 150: return 20
    elif count >= 100: return 15
    elif count >= 50: return 10
    elif count >= 20: return 5
    else: return 0

# --- 書き込みロジック ---
def register_new_inventory(data_list):
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    df = pd.DataFrame(all_records)
    
    current_active = set()
    if not df.empty and 'ステータス' in df.columns:
        active_df = df[df['ステータス'].astype(str).str.strip() == '在庫']
        current_active = set(active_df['シリアルナンバー'].astype(str).tolist())
    
    headers = sheet.row_values(1)
    if not headers: sheet.append_row(EXPECTED_HEADERS)

    rows = []
    skipped = 0
    for s, d in data_list:
        s_str = str(s)
        if s_str in current_active:
            skipped += 1
            continue
        d_str = d.strftime('%Y-%m-%d') if isinstance(d, (datetime.date, datetime.datetime)) else str(d)
        rows.append([s_str, '在庫', d_str, '', '', '', ''])
    
    if rows: sheet.append_rows(rows)
    return len(rows), skipped

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
    comp_str = ""
    if complete_date:
        comp_str = complete_date.strftime('%Y-%m-%d') if isinstance(complete_date, (datetime.date, datetime.datetime)) else str(complete_date)

    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = str(row.get('ステータス', '')).strip()
        if st_val == '在庫' and s in target_set:
            r = i + 2
            cells.append(gspread.Cell(r, col_status, new_status))
            cells.append(gspread.Cell(r, col_end, comp_str))
            cells.append(gspread.Cell(r, col_zone, zone))
            cells.append(gspread.Cell(r, col_price, price))
            if memo: cells.append(gspread.Cell(r, col_memo, memo))
            updated += 1
            
    if cells: sheet.update_cells(cells)
    return updated

def update_dates_bulk(updates_list):
    """日付更新の強化版"""
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    headers = sheet.row_values(1)
    
    if '保有開始日' not in headers: return 0
    col_start = headers.index('保有開始日') + 1
    
    cells = []
    # updates_list = [(serial, date_str), ...]
    updates_map = {str(s): str(d) for s, d in updates_list}
    
    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = str(row.get('ステータス', '')).strip()
        
        # 在庫かつ更新対象に含まれる場合
        if st_val == '在庫' and s in updates_map:
            r = i + 2
            new_date = updates_map[s]
            cells.append(gspread.Cell(r, col_start, new_date))
            
    if cells:
        sheet.update_cells(cells)
        return len(cells)
    return 0

# --- UIパーツ ---
def create_card(row, today):
    start_date = row['保有開始日']
    if pd.isnull(start_date):
        s_str, days, p_days = "-", 0, 99
    else:
        s_str = start_date.strftime('%m/%d')
        days = (today - start_date).days
        p_days = PENALTY_LIMIT_DAYS - days
    
    sn = row['シリアルナンバー']
    last4 = sn[-4:]
    
    if p_days <= 5: 
        c, bg, st_t, bd = "#c62828", "#fff5f5", f"🔥 要返却 (残{p_days}日)", "#e57373"
    elif days <= 3: 
        c, bg, st_t, bd = "#2e7d32", "#f1f8e9", "💎 Bonus", "#81c784"
    else: 
        c, bg, st_t, bd = "#616161", "#ffffff", f"🐢 通常 (残{p_days}日)", "#bdbdbd"
        
    return textwrap.dedent(f"""
    <div style="background:{bg}; border-radius:8px; border-left:8px solid {bd}; padding:12px; margin-bottom:10px; box-shadow:0 2px 5px rgba(0,0,0,0.1);">
        <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:12px; color:{c};">
            <div>{st_t}</div><div>{s_str}〜</div>
        </div>
        <div style="font-size:34px; font-weight:900; color:#212121;">{last4}</div>
        <div style="text-align:right; font-size:10px; color:#999; font-family:monospace;">{sn}</div>
    </div>
    """)

# --- メイン ---
def main():
    st.set_page_config(page_title="Battery Manager V8", page_icon="⚡", layout="wide")
    st.markdown("<style>.stSlider{padding-top:1rem;}</style>", unsafe_allow_html=True)
    today = get_today_jst()

    if 'stocktake_buffer' not in st.session_state: st.session_state['stocktake_buffer'] = []
    if 'parsed_data' not in st.session_state: st.session_state['parsed_data'] = None

    df_all = get_database()
    df_inv = get_active_inventory(df_all)
    df_hist = df_all[df_all['ステータス'] != '在庫'] if not df_all.empty else pd.DataFrame()

    week_earnings = 0
    week_count = 0
    if not df_hist.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        w_df = df_hist[(df_hist['完了日'] >= start_of_week) & (df_hist['ステータス'] == '補充済')]
        week_count = len(w_df)
        week_earnings = int(w_df['金額'].sum())
    cur_bonus = get_vol_bonus(week_count)

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏠 ホーム", "🔍 検索", "📦 在庫", "💰 収益", "📝 棚卸"])

    # 1. ホーム
    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬", f"¥ {week_earnings:,}")
        c2.metric("本数", f"{week_count} 本")
        c3.metric("現在ボナ", f"+{cur_bonus}円")
        st.divider()

        mode = st.radio("モード", ["取出 (登録)", "補充 (確定)"], horizontal=True)
        
        if mode == "取出 (登録)":
            txt = st.text_area("SpotJobsリスト貼付", height=100)
            date_in = st.date_input("基準日 (読取不可時)", value=today)
            if st.button("読込", icon=":material/search:"):
                if txt:
                    parsed = extract_serials_with_date(txt, date_in)
                    st.session_state['parsed_data'] = parsed
                    st.success(f"{len(parsed)} 件 読み込み成功")
            
            if st.session_state['parsed_data']:
                st.dataframe(pd.DataFrame(st.session_state['parsed_data'], columns=["SN","日付"]), hide_index=True)
                if st.button("登録実行", type="primary"):
                    cnt, skip = register_new_inventory(st.session_state['parsed_data'])
                    msg = f"✅ {cnt}件 登録しました"
                    if skip > 0: msg += f" ({skip}件は既に在庫ありのためスキップ)"
                    st.success(msg)
                    st.session_state['parsed_data'] = None
                    import time
                    time.sleep(1)
                    st.rerun()

        else: 
            col_d, col_z = st.columns([1,1])
            date_done = col_d.date_input("補充日", value=today)
            zone = col_z.selectbox("エリア", ZONE_OPTIONS)
            txt = st.text_area("補充リスト貼付", height=100)
            if txt:
                sns = extract_serials_only(txt)
                if sns:
                    price = ZONES[zone] + get_vol_bonus(week_count + len(sns))
                    st.info(f"{len(sns)}件 / 単価 ¥{price}")
                    if st.button("補充確定", type="primary"):
                        cnt = update_status_bulk(sns, "補充済", date_done, zone, price)
                        st.success(f"{cnt}件 更新しました")
                        import time
                        time.sleep(1)
                        st.rerun()

        st.divider()
        st.caption("ピックアップ")
        if not df_inv.empty:
            cols = st.columns(4)
            for i, (_, row) in enumerate(df_inv.head(4).iterrows()):
                cols[i].markdown(create_card(row, today), unsafe_allow_html=True)

    # 2. 検索
    with tab2:
        sn_in = st.number_input("SN下4桁", 0, 9999, 0)
        if sn_in > 0 and not df_all.empty:
            hits = df_all[df_all['シリアルナンバー'].str.endswith(str(sn_in))]
            if not hits.empty:
                st.success(f"{len(hits)}件 ヒット")
                for _, row in hits.iterrows():
                    st.info(f"状態: {row['ステータス']} / 開始: {row['保有開始日']} / 完了: {row['完了日']}")
            else: st.warning("なし")

    # 3. 在庫
    with tab3:
        st.metric("在庫総数", f"{len(df_inv)} 本")
        st.dataframe(df_inv, use_container_width=True)

    # 4. 収益
    with tab4:
        st.metric("今週", f"¥{week_earnings:,}")
        if not df_hist.empty:
            df_g = df_hist[df_hist['ステータス']=='補充済']
            st.dataframe(df_g.sort_values('完了日', ascending=False), use_container_width=True)

    # 5. 棚卸
    with tab5:
        st.subheader("在庫棚卸し")
        
        # バッファ可視化
        cur = st.session_state['stocktake_buffer']
        col_in, col_view = st.columns([1, 1])
        
        with col_in:
            txt_stock = st.text_area("リスト追加 (分割可)", height=150)
            if st.button("リストに追加 ⬇️"):
                if txt_stock:
                    add = extract_serials_with_date(txt_stock, today)
                    st.session_state['stocktake_buffer'].extend(add)
                    uniq = {s:d for s,d in st.session_state['stocktake_buffer']}
                    st.session_state['stocktake_buffer'] = list(uniq.items())
                    st.rerun()
            if st.button("リセット 🗑️"):
                st.session_state['stocktake_buffer'] = []
                st.rerun()

        with col_view:
            st.caption(f"読込済み: {len(cur)}件")
            if cur:
                st.dataframe(pd.DataFrame(cur, columns=["SN", "日付"]), height=200, hide_index=True)

        st.divider()
        
        # 照合ロジック
        if st.button("照合開始", type="primary", use_container_width=True):
            if cur:
                s_map = {s:d for s,d in cur}
                if not df_inv.empty:
                    db_map = dict(zip(df_inv['シリアルナンバー'], df_inv['保有開始日']))
                else: db_map = {}
                
                def fdate(d): return d.strftime('%Y-%m-%d') if pd.notnull(d) else ""
                
                missing_db = []
                date_mis = []
                missing_hand = []
                
                # A: DBにない
                for s, d in s_map.items():
                    if s not in db_map: missing_db.append((s, d))
                    elif fdate(db_map[s]) != d: date_mis.append((s, d))
                
                # B: 手元にない
                for s in db_map.keys():
                    if s not in s_map: missing_hand.append(s)
                
                if not any([missing_db, date_mis, missing_hand]):
                    st.success("🎉 ズレなし (完璧です)")
                else:
                    if missing_db:
                        st.warning(f"🚨 未登録: {len(missing_db)}件")
                        if st.button("一括登録"):
                            cnt, _ = register_new_inventory(missing_db)
                            st.success(f"{cnt}件 登録完了")
                            st.rerun()
                    
                    if date_mis:
                        st.info(f"📅 日付ズレ: {len(date_mis)}件")
                        with st.expander("詳細確認"):
                            st.dataframe(pd.DataFrame(date_mis, columns=["SN", "正しい日付"]), hide_index=True)
                        if st.button("日付を更新する"):
                            cnt = update_dates_bulk(date_mis)
                            st.success(f"{cnt}件の日付を更新しました！")
                            st.rerun()
                            
                    if missing_hand:
                        st.error(f"⚠️ 手元なし: {len(missing_hand)}件")
                        with st.expander("詳細"):
                            st.write(", ".join(missing_hand))
                            if st.button("棚卸削除 (不明に変更)"):
                                update_status_bulk(missing_hand, "不明", today, memo="棚卸削除")
                                st.success("ステータスを不明に変更しました")
                                st.rerun()
            else: st.warning("リストを入力してください")

        # 強制登録ボタン (救済用)
        st.markdown("---")
        if st.button("読込データをすべて強制登録 (救済)", use_container_width=True):
            if cur:
                cnt, skip = register_new_inventory(cur)
                st.success(f"{cnt}件 強制登録しました")
                st.session_state['stocktake_buffer'] = []
                st.rerun()

if __name__ == '__main__':
    main()
