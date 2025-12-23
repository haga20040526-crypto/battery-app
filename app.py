import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import altair as alt
import textwrap
import json

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
    text = text.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    
    date_pattern = re.compile(r'(\d{4})[-/.](\d{2})[-/.](\d{2})')
    serial_pattern = re.compile(r'\b(\d{8})\b')

    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    for i, line in enumerate(lines):
        serials_in_line = serial_pattern.findall(line)
        if not serials_in_line: continue
        
        search_window = lines[max(0, i-2) : min(len(lines), i+3)]
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
    except Exception as e:
        st.error(f"読込エラー: {e}")
        return pd.DataFrame()

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

# --- 書き込み・計算ロジック ---

def recalc_weekly_revenue(sheet, today_date):
    """
    今週の全データを再計算し、最新のボーナス単価で金額を上書きする
    """
    all_records = sheet.get_all_records()
    headers = sheet.row_values(1)
    
    try:
        col_price = headers.index('金額') + 1
    except: return 0

    # 今週の範囲を特定 (月曜〜日曜)
    start_of_week = today_date - datetime.timedelta(days=today_date.weekday())
    end_of_week = start_of_week + datetime.timedelta(days=6)

    # 1. 今週の本数をカウント
    weekly_indices = []
    
    for i, row in enumerate(all_records):
        st_val = str(row.get('ステータス', '')).strip()
        comp_date_str = str(row.get('完了日', ''))
        
        if st_val == '補充済' and comp_date_str:
            try:
                comp_date = datetime.datetime.strptime(comp_date_str, '%Y-%m-%d').date()
                if start_of_week <= comp_date <= end_of_week:
                    weekly_indices.append(i)
            except: pass

    week_count = len(weekly_indices)
    current_bonus = get_vol_bonus(week_count)
    
    # 2. 単価を再計算して更新
    cells_to_update = []
    updated_count = 0
    
    for idx in weekly_indices:
        row = all_records[idx]
        
        # エリア単価
        zone_name = str(row.get('エリア', ''))
        base_price = ZONES.get(zone_name, 70) # デフォルト70
        
        # 早期ボーナス判定
        start_d_str = str(row.get('保有開始日', ''))
        end_d_str = str(row.get('完了日', ''))
        early_bonus = 0
        try:
            s_date = datetime.datetime.strptime(start_d_str, '%Y-%m-%d').date()
            e_date = datetime.datetime.strptime(end_d_str, '%Y-%m-%d').date()
            if (e_date - s_date).days <= 3:
                early_bonus = 10
        except: pass
        
        # 新しい単価
        new_total_price = base_price + current_bonus + early_bonus
        
        # 現在の値と違えば更新リストへ
        current_recorded_price = row.get('金額', 0)
        if current_recorded_price != new_total_price:
            cells_to_update.append(gspread.Cell(idx + 2, col_price, new_total_price))
            updated_count += 1

    if cells_to_update:
        sheet.update_cells(cells_to_update)
        
    return updated_count

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
        row = [sanitize_for_json(s_str), "在庫", sanitize_for_json(d), "", "", "", ""]
        rows.append(row)
    
    if rows:
        try: sheet.append_rows(rows)
        except Exception as e:
            st.error(f"保存エラー: {e}")
            return 0, 0
    return len(rows), skipped

def update_status_bulk(target_serials, new_status, complete_date=None, zone="", price=0, memo=""):
    """ステータス更新 + 週次ボーナス再計算"""
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
    # ここでのpriceは暫定値。直後にrecalc_weekly_revenueで上書きされる
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
        except Exception as e:
            st.error(f"更新エラー: {e}")
            return 0
            
    # ★ここで今週分の金額を再計算して一斉更新
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
    st.set_page_config(page_title="Battery Manager V10", page_icon="⚡", layout="wide")
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
        c3.metric("現在ボナ", f"+{cur_bonus}円/本")
        st.divider()

        mode = st.radio("モード", ["取出 (登録)", "補充 (確定)"], horizontal=True)
        
        if mode == "取出 (登録)":
            txt = st.text_area("SpotJobsリスト貼付", height=100)
            date_in = st.date_input("基準日", value=today)
            if st.button("読込", icon=":material/search:"):
                if txt:
                    parsed = extract_serials_with_date(txt, date_in)
                    st.session_state['parsed_data'] = parsed
                    st.success(f"{len(parsed)} 件 読込")
            
            if st.session_state['parsed_data']:
                st.dataframe(pd.DataFrame(st.session_state['parsed_data'], columns=["SN","日付"]), hide_index=True)
                if st.button("登録実行", type="primary"):
                    cnt, skip = register_new_inventory(st.session_state['parsed_data'])
                    if cnt > 0:
                        st.success(f"✅ {cnt}件 登録完了")
                        st.session_state['parsed_data'] = None
                        import time
                        time.sleep(1)
                        st.rerun()
                    else: st.warning("登録なし (すべて重複)")

        else: 
            col_d, col_z = st.columns([1,1])
            date_done = col_d.date_input("補充日", value=today)
            zone = col_z.selectbox("エリア", ZONE_OPTIONS)
            txt = st.text_area("補充リスト貼付", height=100)
            if txt:
                sns = extract_serials_only(txt)
                if sns:
                    # 予測表示 (ここでの表示は参考値、確定時に全件再計算される)
                    base = ZONES[zone]
                    new_count = week_count + len(sns)
                    new_bonus = get_vol_bonus(new_count)
                    st.info(f"{len(sns)}件検出 / 確定後の全件ボーナス: +{new_bonus}円 (総数{new_count}本)")
                    
                    if st.button("補充確定 (遡及計算)", type="primary"):
                        cnt = update_status_bulk(sns, "補充済", date_done, zone, base)
                        st.success(f"{cnt}件 更新 & 今週分の単価を再計算しました")
                        import time
                        time.sleep(1)
                        st.rerun()

        st.divider()
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
        st.metric("在庫数", f"{len(df_inv)}")
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
        cur = st.session_state['stocktake_buffer']
        
        c1, c2 = st.columns([1,1])
        with c1:
            txt_stock = st.text_area("リスト追加")
            if st.button("リストに追加"):
                if txt_stock:
                    add = extract_serials_with_date(txt_stock, today)
                    st.session_state['stocktake_buffer'].extend(add)
                    uniq = {s:d for s,d in st.session_state['stocktake_buffer']}
                    st.session_state['stocktake_buffer'] = list(uniq.items())
                    st.rerun()
            if st.button("クリア"):
                st.session_state['stocktake_buffer'] = []
                st.rerun()
        
        with c2:
            st.caption(f"読込: {len(cur)}件")
            if cur: st.dataframe(pd.DataFrame(cur, columns=["SN","日付"]), height=150, hide_index=True)

        st.divider()
        c_act1, c_act2 = st.columns(2)
        with c_act1:
            if st.button("照合＆登録・更新", type="primary", use_container_width=True):
                if cur:
                    s_map = {s:d for s,d in cur}
                    db_map = {}
                    if not df_inv.empty:
                        db_map = dict(zip(df_inv['シリアルナンバー'], df_inv['保有開始日']))
                    
                    def fdate(d): return d.strftime('%Y-%m-%d') if pd.notnull(d) else ""
                    
                    missing_db = []
                    date_mis = []
                    for s, d in s_map.items():
                        if s not in db_map: missing_db.append((s, d))
                        elif fdate(db_map[s]) != d: date_mis.append((s, d))
                    
                    msg = []
                    if missing_db:
                        cnt, _ = register_new_inventory(missing_db)
                        msg.append(f"新規: {cnt}件")
                    if date_mis:
                        cnt = update_dates_bulk(date_mis)
                        msg.append(f"日付更新: {cnt}件")
                    
                    if msg: st.success(" / ".join(msg))
                    else: st.info("変更なし")
                    
                    import time
                    time.sleep(1)
                    st.rerun()
                else: st.warning("リストなし")

        with c_act2:
            if st.button("強制全件登録 (救済)", use_container_width=True):
                if cur:
                    cnt, skip = register_new_inventory(cur)
                    st.success(f"{cnt}件 強制登録")
                    import time
                    time.sleep(1)
                    st.rerun()

if __name__ == '__main__':
    main()
