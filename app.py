import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import altair as alt
import uuid
import time
import json
import threading
import os

# --- 定数・設定 ---
PENALTY_LIMIT_DAYS = 28
NEW_SHEET_NAME = 'database' 
# ジョブID列を含むヘッダー定義
EXPECTED_HEADERS = ['シリアルナンバー', 'ステータス', '保有開始日', '完了日', 'エリア', '金額', '備考', 'ジョブID']
ANALYTICS_CACHE_FILE = 'analytics_cache.json'

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
        
        # カラム不足の補正
        current_cols = df.columns.tolist()
        if 'ジョブID' not in current_cols:
            sheet.update_cell(1, len(current_cols) + 1, 'ジョブID')
            df['ジョブID'] = ""

        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        if 'ステータス' in df.columns:
            df['ステータス'] = df['ステータス'].astype(str).str.strip()
        
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

# --- 分析モジュール (Analytics Logic V1.4) ---

def calculate_kpi_for_period(df_subset):
    if len(df_subset) == 0:
        return {"ebr": 0, "rpd": 0, "ahd": 0, "count": 0, "revenue": 0, "avg_price": 0}
    
    early_count = len(df_subset[df_subset['holding_days'] <= 3])
    ebr = (early_count / len(df_subset)) * 100
    
    total_rev = df_subset['金額'].sum()
    total_hold_days = df_subset['holding_days'].sum()
    rpd = total_rev / total_hold_days if total_hold_days > 0 else 0
    ahd = df_subset['holding_days'].mean()
    avg_price = df_subset['金額'].mean()

    return {
        "ebr": ebr, "rpd": rpd, "ahd": ahd, 
        "count": len(df_subset), "revenue": total_rev, "avg_price": avg_price
    }

def calculate_analytics_logic(df):
    if df.empty: return {}
    df['completed_at'] = pd.to_datetime(df['完了日'], errors='coerce')
    df['acquired_at'] = pd.to_datetime(df['保有開始日'], errors='coerce')
    
    completed_df = df[df['ステータス'] == '補充済'].copy()
    completed_df = completed_df.dropna(subset=['completed_at', 'acquired_at'])
    completed_df['holding_days'] = (completed_df['completed_at'] - completed_df['acquired_at']).dt.days
    
    today = datetime.datetime.now()
    current_start = today - datetime.timedelta(days=7)
    previous_start = today - datetime.timedelta(days=14)
    
    current_df = completed_df[completed_df['completed_at'] >= current_start]
    prev_df = completed_df[(completed_df['completed_at'] >= previous_start) & (completed_df['completed_at'] < current_start)]
    
    cur_metrics = calculate_kpi_for_period(current_df)
    prev_metrics = calculate_kpi_for_period(prev_df)
    
    input_df = df[df['acquired_at'] >= current_start]
    input_count = len(input_df)
    output_count = cur_metrics['count']
    io_balance = (input_count / output_count) if output_count > 0 else 0
    
    month_start = today - datetime.timedelta(days=30)
    month_df = completed_df[completed_df['completed_at'] >= month_start].copy()
    raw_holding_days = month_df['holding_days'].tolist()

    month_df['weekday'] = month_df['completed_at'].dt.day_name()
    heatmap_series = month_df.groupby('weekday').size()
    heatmap_data = [{'weekday': wd, 'count': int(count)} for wd, count in heatmap_series.items()]

    three_months_ago = today - datetime.timedelta(days=90)
    trend_df = completed_df[completed_df['completed_at'] >= three_months_ago].copy()
    trend_df['week'] = trend_df['completed_at'].dt.to_period('W').astype(str)
    trend_series = trend_df.groupby('week')['holding_days'].mean()
    trend_data = [{'week': w, 'avg_days': round(d, 2)} for w, d in trend_series.items()]

    return {
        "scorecard": {"current": cur_metrics, "previous": prev_metrics},
        "tactical": {"io_balance": round(io_balance, 2), "input_count": input_count, "output_count": output_count},
        "histogram_raw": raw_holding_days,
        "heatmap": heatmap_data,
        "trend": trend_data,
        "updated_at": today.strftime('%Y-%m-%d %H:%M:%S')
    }

def update_analytics_background():
    def task():
        try:
            df = get_database()
            if df.empty: return
            data = calculate_analytics_logic(df)
            with open(ANALYTICS_CACHE_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Background update failed: {e}")
    thread = threading.Thread(target=task)
    thread.start()

def load_analytics_cache():
    if not os.path.exists(ANALYTICS_CACHE_FILE):
        return None
    try:
        with open(ANALYTICS_CACHE_FILE, 'r') as f:
            return json.load(f)
    except:
        return None

# --- 書き込み・計算ロジック ---

def register_new_inventory(data_list):
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    df = pd.DataFrame(all_records)
    
    current_active_serials = set()
    if not df.empty and 'ステータス' in df.columns:
        active_df = df[df['ステータス'].astype(str).str.strip().isin(['在庫', '出庫中'])]
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
        row = [sanitize_for_json(s_str), "在庫", sanitize_for_json(d), "", "", "", "", ""]
        rows.append(row)
    
    if rows:
        try: 
            sheet.append_rows(rows)
            update_analytics_background()
        except: return 0, 0
    return len(rows), skipped

def register_past_bulk(date_obj, count, total_amount, zone, memo="", job_id=""):
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
        row = [dummy_sn, "補充済", "", date_str, zone, amount, memo, job_id]
        rows.append(row)
    if rows: 
        sheet.append_rows(rows)
        update_analytics_background()

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

def update_status_bulk(target_serials, new_status, complete_date=None, zone="", price=0, memo="", job_id=""):
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
        col_job = headers.index('ジョブID') + 1 if 'ジョブID' in headers else None
    except: return 0

    cells = []
    target_set = set(str(s) for s in target_serials)
    
    # --- Strict Validation Start ---
    # まず全レコードからターゲットSNの現在のステータスを特定
    sn_status_map = {}
    for row in all_records:
        r_sn = str(row.get('シリアルナンバー', ''))
        if r_sn in target_set:
            sn_status_map[r_sn] = str(row.get('ステータス', '')).strip()
    
    # 検証1: DBに存在しないSNがあるか
    missing_sns = target_set - set(sn_status_map.keys())
    if missing_sns:
        return {"error": True, "msg": f"未登録のバッテリーが含まれています: {', '.join(missing_sns)}"}
    
    # 検証2: ステータスが対象外(在庫/出庫中以外)のものがあるか
    permitted_statuses = ['在庫', '出庫中']
    invalid_sns = []
    for sn, st_val in sn_status_map.items():
        if st_val not in permitted_statuses:
            invalid_sns.append(f"{sn}({st_val})")
            
    if invalid_sns:
        return {"error": True, "msg": f"対象外ステータスのバッテリーが含まれています: {', '.join(invalid_sns)}"}
    # --- Strict Validation End ---

    comp_str = sanitize_for_json(complete_date)
    safe_price = int(price)

    # バリデーション通過後、更新処理
    updated = 0
    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        if s in target_set:
            r = i + 2
            cells.append(gspread.Cell(r, col_status, new_status))
            cells.append(gspread.Cell(r, col_end, comp_str))
            cells.append(gspread.Cell(r, col_zone, zone))
            cells.append(gspread.Cell(r, col_price, safe_price))
            if memo: cells.append(gspread.Cell(r, col_memo, memo))
            if col_job and job_id: cells.append(gspread.Cell(r, col_job, job_id))
            updated += 1
            
    if cells:
        try: sheet.update_cells(cells)
        except: return {"error": True, "msg": "DB更新エラー"}
    
    if updated > 0 and new_status == '補充済' and complete_date:
        recalc_weekly_revenue(sheet, complete_date)
        update_analytics_background()

    return {"error": False, "count": updated}

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
    elif status == '出庫中':
        c, bg, st_t, bd = "#f57c00", "#fff3e0", "🚚 出庫中", "#ff9800"
        date_label = f"取得: {s_str}"
        main_text = last4
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

    html = f"""
    <div style="background:{bg}; border-radius:8px; border-left:6px solid {bd}; padding:10px; margin-bottom:8px; box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <div style="display:flex; justify-content:space-between; font-size:11px; font-weight:bold; color:{c};">
            <div>{st_t}</div><div>{date_label}</div>
        </div>
        <div style="font-size:28px; font-weight:900; color:#212121; margin-top:2px; letter-spacing:1px;">{main_text}</div>
        <div style="text-align:right; font-size:9px; color:#999; font-family:monospace;">{sn}</div>
    </div>
    """
    return html

# --- メイン ---
def main():
    st.set_page_config(page_title="Battery Manager V34", page_icon="⚡", layout="wide")
    
    # ヘッダー
    st.markdown("""<div style='display: flex; align-items: center; border-bottom: 2px solid #ff7043; padding-bottom: 10px; margin-bottom: 20px;'><div style='font-size: 40px; margin-right: 15px;'>⚡</div><div><h1 style='margin: 0; padding: 0; font-size: 32px; color: #333; font-family: sans-serif; letter-spacing: -1px;'>Battery Manager</h1><div style='font-size: 14px; color: #757575;'>Pure Instrument <span style='color: #ff7043; font-weight: bold; margin-left:8px;'>V34 (Strict & JobView)</span></div></div></div>""", unsafe_allow_html=True)

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
    last_week_earnings = 0
    week_count = 0
    next_bonus_at = 20
    
    if not df_hist.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        last_week_start = start_of_week - datetime.timedelta(days=7)
        
        df_hist['comp_date'] = pd.to_datetime(df_hist['完了日'], errors='coerce')
        
        w_df = df_hist[
            (df_hist['comp_date'].dt.date >= start_of_week) & 
            (df_hist['ステータス'] == '補充済')
        ].copy()
        
        lw_df = df_hist[
            (df_hist['comp_date'].dt.date >= last_week_start) & 
            (df_hist['comp_date'].dt.date < start_of_week) & 
            (df_hist['ステータス'] == '補充済')
        ].copy()

        count_mask = w_df.apply(lambda x: 'ボーナス' not in str(x['備考']), axis=1)
        week_count = len(w_df[count_mask])
        week_earnings = int(w_df['金額'].sum())
        last_week_earnings = int(lw_df['金額'].sum())
        
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

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🏠 ホーム", "🔍 検索", "📦 在庫", "💰 収益", "📝 棚卸", "📊 分析"])

    # 1. ホーム
    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬", f"¥ {week_earnings:,}", delta=f"{week_earnings - last_week_earnings:,} 円 (先週比)")
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
                        now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                        auto_job_id = f"J{now_str}"
                        
                        # V34: エラーハンドリング追加
                        res = update_status_bulk(sns, "補充済", date_done, zone, base, job_id=auto_job_id)
                        if isinstance(res, dict) and res.get('error'):
                            st.error(f"⛔️ エラー: {res['msg']}")
                        else:
                            cnt = res['count'] if isinstance(res, dict) else res
                            st.success(f"✅ {cnt}件 更新完了 (ID: {auto_job_id})")
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

    # 2. 検索
    with tab2:
        date_options = ["指定なし"]
        date_map = {}
        if not df_inv.empty:
            unique_dates = sorted(df_inv['保有開始日'].unique(), reverse=True)
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

        results = pd.DataFrame()
        
        if sel_date != "指定なし":
            target_date = date_map[sel_date]
            results = df_inv[df_inv['保有開始日'] == target_date].copy()
            if sn_in > 0:
                results = results[results['シリアルナンバー'].str.endswith(str(sn_in))]
            
            if not results.empty:
                st.success(f"{len(results)}件 (保有日: {sel_date})")
                for _, row in results.iterrows():
                    st.markdown(create_card(row, today), unsafe_allow_html=True)
            else:
                st.warning("該当なし")

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
        st.metric("今週", f"¥{week_earnings:,}", delta=f"{week_earnings - last_week_earnings:,} 円 (先週比)")
        
        with st.expander("➕ 過去データの登録"):
            with st.form("manual_past_reg"):
                c1, c2 = st.columns(2)
                p_date = c1.date_input("完了日")
                p_count = c2.number_input("数量", min_value=1, value=1)
                p_amount = c1.number_input("合計金額", step=10)
                p_zone = c2.selectbox("エリア", ZONE_OPTIONS)
                p_memo = st.text_input("備考", placeholder="ボーナスなど")
                
                if st.form_submit_button("登録"):
                    now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                    auto_job_id = f"J{now_str}"
                    reg_cnt = register_past_bulk(p_date, p_count, p_amount, p_zone, p_memo, job_id=auto_job_id)
                    st.success(f"{reg_cnt}行 登録完了 (ID: {auto_job_id})")
                    time.sleep(1)
                    st.rerun()
        
        st.divider()
        st.subheader("📊 履歴タイムライン (Job Group View)")

        if not df_hist.empty:
            df_done = df_hist[df_hist['ステータス'] == '補充済'].copy()
            if not df_done.empty:
                # JobIDでグルーピング（JobIDがないものは空文字として扱う）
                # 並び順: JobIDの降順（時系列）
                df_done['ジョブID'] = df_done['ジョブID'].fillna('')
                # JobIDがない場合は日付で代用グルーピングするためのキー作成
                df_done['group_key'] = df_done.apply(lambda x: x['ジョブID'] if x['ジョブID'] else f"NO-JOB-{x['完了日']}", axis=1)
                
                # グループ化して集計
                jobs = []
                grouped = df_done.groupby('group_key')
                
                for key, group in grouped:
                    first_row = group.iloc[0]
                    job_id = first_row['ジョブID']
                    date_val = first_row['完了日']
                    area_val = first_row['エリア']
                    total_amt = group['金額'].sum()
                    count = len(group)
                    
                    # SNリスト
                    sn_list = group['シリアルナンバー'].tolist()
                    
                    jobs.append({
                        'key': key, # ソート用
                        'job_id': job_id,
                        'date': date_val,
                        'area': area_val,
                        'amount': total_amt,
                        'count': count,
                        'sns': sn_list
                    })
                
                # ソート (Keyの降順 = 新しい順)
                jobs.sort(key=lambda x: x['key'], reverse=True)
                
                for j in jobs:
                    # カード表示
                    job_label = j['job_id'] if j['job_id'] else "Legacy Job (No ID)"
                    
                    # カスタムカードHTML
                    card_html = f"""
                    <div style="background:#ffffff; border:1px solid #e0e0e0; border-radius:8px; padding:12px; margin-bottom:5px; border-left: 5px solid #1565c0;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <div>
                                <div style="font-size:12px; color:#757575; font-weight:bold;">{j['date']} | {j['area']}</div>
                                <div style="font-size:16px; color:#212121; font-weight:bold;">{job_label}</div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-size:20px; font-weight:900; color:#1565c0;">¥{j['amount']:,}</div>
                                <div style="font-size:11px; color:#757575;">{j['count']}本</div>
                            </div>
                        </div>
                    </div>
                    """
                    st.markdown(card_html, unsafe_allow_html=True)
                    with st.expander(f"詳細を見る ({len(j['sns'])}本)"):
                        st.write(", ".join(j['sns']))

    # 5. 棚卸
    with tab5:
        st.subheader("在庫棚卸し")
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
                        time.sleep(1)
                        st.rerun()
            with c_act2:
                st.markdown(f"**② 消失・エラー検知: {len(ghosts)}件**")
                if ghosts:
                    st.warning("在庫差異あり")
                    with st.expander("詳細"): st.write(ghosts)
                    if st.button("一括「補充エラー」にする"):
                        # V34: 棚卸しの一括エラー処理もエラーハンドリング対応
                        res = update_status_bulk(ghosts, "補充エラー", today, "", 0, "棚卸検知")
                        if isinstance(res, dict) and res.get('error'):
                            st.error(res['msg'])
                        else:
                            cnt = res['count'] if isinstance(res, dict) else res
                            st.success(f"{cnt}件 を在庫から除外しました")
                            time.sleep(1)
                            st.rerun()
                else: st.success("差異なし")

    # 6. 分析 (Analytics)
    with tab6:
        st.subheader("📊 Analytics: Pure Instrument")
        
        # キャッシュからデータ読み込み
        analytics_data = load_analytics_cache()
        
        if not analytics_data:
            st.info("データ集計待ち... 任意のジョブを実行すると初回計算が始まります。")
            if st.button("強制リフレッシュ"):
                update_analytics_background()
                st.success("計算開始。リロードしてください。")
        else:
            sc = analytics_data.get('scorecard', {})
            cur = sc.get('current', {})
            prev = sc.get('previous', {})
            tactical = analytics_data.get('tactical', {})

            # --- Section 1: Head-Up Display (Comparison) ---
            st.markdown("#### 1. The Head-Up Display (vs Last Week)")
            c_k1, c_k2, c_k3 = st.columns(3)
            
            # Early Bonus Rate
            ebr_cur = cur.get('ebr', 0)
            ebr_prev = prev.get('ebr', 0)
            c_k1.metric(
                label="🏆 Early Bonus Rate (勝率)",
                value=f"{ebr_cur:.1f}%",
                delta=f"{ebr_cur - ebr_prev:.1f}% (vs LW)"
            )
            # RPD
            rpd_cur = cur.get('rpd', 0)
            rpd_prev = prev.get('rpd', 0)
            c_k2.metric(
                label="💰 RPD (資産回転速度)",
                value=f"¥{int(rpd_cur)}/day",
                delta=f"{int(rpd_cur - rpd_prev)} (vs LW)"
            )
            # Avg Holding Days
            ahd_cur = cur.get('ahd', 0)
            ahd_prev = prev.get('ahd', 0)
            c_k3.metric(
                label="⚡ Avg. Holding Days (鮮度)",
                value=f"{ahd_cur:.1f} days",
                delta=f"{ahd_cur - ahd_prev:.1f} (vs LW)",
                delta_color="inverse" # 増える＝悪化なので色反転
            )
            st.divider()

            # --- Section 2: Tactical Metrics ---
            st.markdown("#### 2. Tactical Metrics")
            t_c1, t_c2 = st.columns(2)
            
            # APU (Avg Price Unit)
            apu = cur.get('avg_price', 0)
            apu_delta = apu - 70 # エリアD基準(70円)との乖離
            t_c1.metric(
                label="💎 APU (平均単価)",
                value=f"¥{int(apu)}",
                delta=f"{int(apu_delta)} vs Std(¥70)"
            )
            
            # I/O Balance
            io = tactical.get('io_balance', 0)
            t_c2.metric(
                label="⚖️ I/O Balance (入庫/出庫)",
                value=f"{io:.2f}",
                delta="Overstock" if io > 1.1 else ("Drain" if io < 0.9 else "Balanced"),
                delta_color="off"
            )
            st.caption(f"Input: {tactical.get('input_count')} / Output: {tactical.get('output_count')} (Last 7 Days)")
            st.divider()

            # --- Section 3: Cycle Histogram + Density ---
            st.markdown("#### 3. Cycle Distribution (Histogram + Density)")
            raw_days = analytics_data.get('histogram_raw', [])
            if raw_days:
                hist_source = pd.DataFrame({'days': raw_days})
                hist_source['zone'] = hist_source['days'].apply(
                    lambda x: 'A(0-3)' if x <= 3 else ('B(4-22)' if x <= 22 else 'C(23+)')
                )

                base = alt.Chart(hist_source).encode(x=alt.X('days', title='保有日数', bin=alt.Bin(maxbins=30)))

                # 1. Histogram
                bars = base.mark_bar(opacity=0.6).encode(
                    y=alt.Y('count()', title='本数'),
                    color=alt.Color('zone', scale=alt.Scale(range=['#4caf50', '#ffeb3b', '#f44336']))
                )
                
                # 2. Density Curve
                density = alt.Chart(hist_source).transform_density(
                    'days',
                    as_=['days', 'density'],
                ).mark_line(color='white', strokeWidth=3).encode(
                    x='days:Q',
                    y=alt.Y('density:Q', axis=None) # 軸は隠す
                )
                
                # Peak Indicator logic (Altair上では難しいので簡易的に平均線を表示)
                rule = alt.Chart(hist_source).mark_rule(color='red', strokeDash=[5,5]).encode(
                    x='mean(days):Q'
                )

                st.altair_chart((bars + density + rule).resolve_scale(y='independent'), use_container_width=True)
            
            # --- Section 4: Activity Heatmap ---
            st.markdown("#### 4. Activity Heatmap (Past 30 Days)")
            hm_d = analytics_data.get('heatmap', [])
            if hm_d:
                hm_df = pd.DataFrame(hm_d)
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                chart_heat = alt.Chart(hm_df).mark_rect().encode(
                    x=alt.X('weekday', sort=days_order, title=None),
                    y=alt.Y('count', title='完了本数'), # ヒートマップ的表現だが棒の高さも利用
                    color=alt.Color('count', scale=alt.Scale(scheme='inferno'), title='Intensity'),
                    tooltip=['weekday', 'count']
                ).properties(height=200)
                st.altair_chart(chart_heat, use_container_width=True)

            # --- Section 5: Trend ---
            st.markdown("#### 5. Efficiency Trend (90 Days)")
            tr_d = analytics_data.get('trend', [])
            if tr_d:
                tr_df = pd.DataFrame(tr_d)
                chart_trend = alt.Chart(tr_df).mark_line(point=True).encode(
                    x=alt.X('week', title='週'),
                    y=alt.Y('avg_days', title='平均保有日数', scale=alt.Scale(zero=False)),
                    tooltip=['week', 'avg_days']
                ).properties(height=250)
                st.altair_chart(chart_trend, use_container_width=True)

            st.caption(f"Last Updated: {analytics_data.get('updated_at', '-')}")

if __name__ == '__main__':
    main()
