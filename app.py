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

# --- 分析モジュール (Analytics Logic) ---

def calculate_analytics_logic(df):
    """
    データフレームから分析用データを計算する（バックグラウンド実行用）
    """
    if df.empty: return {}

    # 日付変換
    df['completed_at'] = pd.to_datetime(df['完了日'], errors='coerce')
    df['acquired_at'] = pd.to_datetime(df['保有開始日'], errors='coerce')
    
    # 完了済みデータの抽出
    completed_df = df[df['ステータス'] == '補充済'].copy()
    completed_df = completed_df.dropna(subset=['completed_at', 'acquired_at'])
    completed_df['holding_days'] = (completed_df['completed_at'] - completed_df['acquired_at']).dt.days

    # --- 1. KPI計算 ---
    # Early Bonus Rate (直近30日)
    today = datetime.datetime.now()
    month_ago = today - datetime.timedelta(days=30)
    recent_df = completed_df[completed_df['completed_at'] >= month_ago]
    
    early_rate = 0
    if len(recent_df) > 0:
        early_count = len(recent_df[recent_df['holding_days'] <= 3])
        early_rate = (early_count / len(recent_df)) * 100

    # RPD (Revenue Per Day)
    total_rev = completed_df['金額'].sum()
    total_days = completed_df['holding_days'].sum()
    # 0日保有も1日とみなすか、そのまま計算するか。ここでは0除算回避のみ。
    if total_days == 0: total_days = 1 
    rpd = total_rev / total_days

    # Avg Holding Days
    avg_holding = completed_df['holding_days'].mean() if len(completed_df) > 0 else 0

    # --- 2. ヒストグラムデータ ---
    # Zone A(0-3), B(4-22), C(23+)
    hist_counts = completed_df['holding_days'].value_counts().sort_index().to_dict()
    # キーを文字列化してJSON保存可能に
    hist_data = {str(k): int(v) for k, v in hist_counts.items()}

    # --- 3. ヒートマップデータ (曜日別活動量) ---
    # 時間データがないため、曜日ごとの完了数で代用
    completed_df['weekday'] = completed_df['completed_at'].dt.day_name()
    # 時間帯はダミー(Day)とするか、将来の拡張に備える
    completed_df['time_zone'] = 'Day' 
    heatmap_series = completed_df.groupby(['weekday', 'time_zone']).size()
    heatmap_data = []
    for (wd, tz), count in heatmap_series.items():
        heatmap_data.append({'weekday': wd, 'time_zone': tz, 'count': int(count)})

    # --- 4. 推移分析 (週次 平均保有日数) ---
    three_months_ago = today - datetime.timedelta(days=90)
    trend_df = completed_df[completed_df['completed_at'] >= three_months_ago].copy()
    trend_df['week'] = trend_df['completed_at'].dt.to_period('W').astype(str)
    trend_series = trend_df.groupby('week')['holding_days'].mean()
    trend_data = [{'week': w, 'avg_days': round(d, 2)} for w, d in trend_series.items()]

    return {
        "kpi": {
            "early_bonus_rate": round(early_rate, 1),
            "rpd": round(rpd, 1),
            "avg_holding_days": round(avg_holding, 1)
        },
        "histogram": hist_data,
        "heatmap": heatmap_data,
        "trend": trend_data,
        "updated_at": today.strftime('%Y-%m-%d %H:%M:%S')
    }

def update_analytics_background():
    """
    バックグラウンドスレッドでKPIを再計算してJSONに保存
    """
    def task():
        # DBから最新データを取得（スレッド内で安全に行うため再取得）
        # ※StreamlitのSecretsはスレッド内でも参照可能
        try:
            df = get_database()
            if df.empty: return
            
            data = calculate_analytics_logic(df)
            with open(ANALYTICS_CACHE_FILE, 'w') as f:
                json.dump(data, f)
            # print("Analytics updated in background.")
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

# --- 書き込み・計算ロジック (トリガー追加版) ---

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
            # ★トリガー: 分析データのバックグラウンド更新
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
        # ★トリガー: 分析データのバックグラウンド更新
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
    updated = 0
    target_set = set(str(s) for s in target_serials)
    comp_str = sanitize_for_json(complete_date)
    safe_price = int(price)
    
    permitted_statuses = ['在庫', '出庫中']

    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = str(row.get('ステータス', '')).strip()
        
        if st_val in permitted_statuses and s in target_set:
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
        except: return 0
    
    if updated > 0 and new_status == '補充済' and complete_date:
        recalc_weekly_revenue(sheet, complete_date)
        # ★トリガー: 分析データのバックグラウンド更新
        update_analytics_background()

    return updated

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

def create_history_card(row):
    comp_date = pd.to_datetime(row['完了日']).strftime('%m/%d')
    amount = row['金額']
    memo = str(row['備考'])
    sn = str(row['シリアルナンバー'])
    zone = str(row['エリア'])
    job_id = str(row.get('ジョブID', ''))
    
    if "ボーナス" in memo or "差額" in memo:
        job_type = "ボーナス/調整"
        icon = "✨"
        bg = "#fff8e1"
        border = "#ffb300"
        sn_disp = memo
    elif "エラー" in memo:
        job_type = "エラー処理"
        icon = "⚠️"
        bg = "#ffebee"
        border = "#ef5350"
        sn_disp = f"SN: {sn[-4:]}"
    else:
        job_type = "バッテリー補充"
        icon = "🔋"
        bg = "#ffffff"
        border = "#e0e0e0"
        sn_disp = f"SN: {sn[-4:]} ({zone})"
        if job_id:
            # ジョブIDを表示
            sn_disp += f" <span style='color:#1565c0; font-size:10px;'>[{job_id}]</span>"

    html = f"""<div style="background:{bg}; border:1px solid {border}; border-radius:8px; padding:10px 14px; margin-bottom:8px; display:flex; align-items:center; box-shadow: 0 1px 2px rgba(0,0,0,0.05);"><div style="font-size:24px; margin-right:12px;">{icon}</div><div style="flex-grow:1;"><div style="font-size:13px; font-weight:bold; color:#424242;">{job_type}</div><div style="font-size:11px; color:#757575;">{comp_date} | {sn_disp}</div></div><div style="text-align:right;"><div style="font-size:16px; font-weight:900; color:#212121;">¥{amount}</div></div></div>"""
    return html

# --- メイン ---
def main():
    st.set_page_config(page_title="Battery Manager V32", page_icon="⚡", layout="wide")
    
    # ヘッダー
    st.markdown("""<div style='display: flex; align-items: center; border-bottom: 2px solid #ff7043; padding-bottom: 10px; margin-bottom: 20px;'><div style='font-size: 40px; margin-right: 15px;'>⚡</div><div><h1 style='margin: 0; padding: 0; font-size: 32px; color: #333; font-family: sans-serif; letter-spacing: -1px;'>Battery Manager</h1><div style='font-size: 14px; color: #757575;'>Recorder to Strategist <span style='color: #ff7043; font-weight: bold; margin-left:8px;'>V32</span></div></div></div>""", unsafe_allow_html=True)

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
                        # 自動ジョブID生成
                        now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                        auto_job_id = f"J{now_str}"
                        
                        cnt = update_status_bulk(sns, "補充済", date_done, zone, base, job_id=auto_job_id)
                        if cnt > 0:
                            st.success(f"{cnt}件 更新しました (ID: {auto_job_id})")
                        else:
                            st.warning("更新できませんでした。在庫または出庫中のステータスか確認してください。")
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
                    # 自動ジョブID生成
                    now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                    auto_job_id = f"J{now_str}"
                    
                    reg_cnt = register_past_bulk(p_date, p_count, p_amount, p_zone, p_memo, job_id=auto_job_id)
                    st.success(f"{reg_cnt}行 登録完了 (ID: {auto_job_id})")
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
                st.subheader("📊 履歴タイムライン")
                
                if 'orig_index' not in df_wk.columns:
                    df_wk['orig_index'] = df_wk.index
                recent_history = df_wk.sort_values(by=['date', 'orig_index'], ascending=[False, False]).head(30)
                
                for _, row in recent_history.iterrows():
                    st.markdown(create_history_card(row), unsafe_allow_html=True)

                st.divider()
                st.subheader("📈 週次比較")
                
                chart_data = weekly_agg.sort_values('week_start', ascending=True)
                base = alt.Chart(chart_data).encode(x=alt.X('Label', sort=None, title='週'))
                bar = base.mark_bar(color='#ffcc80').encode(
                    y=alt.Y('total_amount', title='金額', axis=alt.Axis(titleColor='#ff7043')),
                    tooltip=['Label', 'total_amount', 'count']
                )
                line = base.mark_line(color='#ff7043', strokeWidth=3).encode(
                    y=alt.Y('count', title='本数', axis=alt.Axis(titleColor='#ff7043'))
                )
                points = base.mark_circle(color='#ff7043', size=60).encode(
                    y=alt.Y('count', axis=None)
                )
                st.altair_chart(alt.layer(bar, line + points).resolve_scale(y='independent').properties(height=300), use_container_width=True)
                
                st.markdown("##### 📅 週間集計")
                display_df = weekly_agg[['Label', 'total_amount', 'count']].rename(
                    columns={'Label': '週 (月曜開始)', 'total_amount': '合計金額 (円)', 'count': '本数 (本)'}
                )
                st.dataframe(display_df, hide_index=True, use_container_width=True)

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
                        cnt = update_status_bulk(ghosts, "補充エラー", today, "", 0, "棚卸検知")
                        st.success(f"{cnt}件 を在庫から除外しました")
                        time.sleep(1)
                        st.rerun()
                else: st.success("差異なし")

    # 6. 分析 (Analytics)
    with tab6:
        st.subheader("📊 Analytics: Strategist Mode")
        
        # キャッシュからデータ読み込み
        analytics_data = load_analytics_cache()
        
        if not analytics_data:
            st.info("現在データを集計中です。何らかのジョブ（補充・登録など）を行うと初回計算が走ります。")
            if st.button("今すぐ強制集計 (少し時間がかかります)"):
                update_analytics_background()
                st.success("バックグラウンド集計を開始しました。ページをリロードしてください。")
        else:
            # --- Section 1: KPI Scorecard ---
            st.markdown("#### 1. The Head-Up Display")
            kpi = analytics_data.get('kpi', {})
            c_k1, c_k2, c_k3 = st.columns(3)
            
            # Early Bonus Rate
            ebr = kpi.get('early_bonus_rate', 0)
            c_k1.metric(
                label="🏆 Early Bonus Rate",
                value=f"{ebr}%",
                delta="Target: 80%",
                delta_color="normal" if ebr >= 80 else "inverse"
            )
            # RPD
            rpd = kpi.get('rpd', 0)
            c_k2.metric(
                label="💰 RPD (資産回転速度)",
                value=f"¥{rpd}/day",
                help="1日あたり何円の価値を生んでいるか"
            )
            # Avg Holding Days
            ahd = kpi.get('avg_holding_days', 0)
            c_k3.metric(
                label="⚡ Avg. Holding Days",
                value=f"{ahd} days",
                delta="Limit: 3.0 days",
                delta_color="inverse"
            )
            st.divider()

            # --- Section 2: Histogram ---
            st.markdown("#### 2. Cycle Histogram (在庫サイクル分布)")
            hist_d = analytics_data.get('histogram', {})
            if hist_d:
                hist_df = pd.DataFrame(list(hist_d.items()), columns=['days_str', 'count'])
                hist_df['days'] = pd.to_numeric(hist_df['days_str'])
                hist_df['zone'] = hist_df['days'].apply(
                    lambda x: '🟢 Zone A (Ideal)' if x <= 3 else ('🟡 Zone B (Normal)' if x <= 22 else '🔴 Zone C (Danger)')
                )
                
                chart_hist = alt.Chart(hist_df).mark_bar().encode(
                    x=alt.X('days', title='保有日数'),
                    y=alt.Y('count', title='本数'),
                    color=alt.Color('zone', scale=alt.Scale(
                        domain=['🟢 Zone A (Ideal)', '🟡 Zone B (Normal)', '🔴 Zone C (Danger)'],
                        range=['#4caf50', '#ffeb3b', '#f44336']
                    )),
                    tooltip=['days', 'count', 'zone']
                ).properties(height=250)
                st.altair_chart(chart_hist, use_container_width=True)
            
            # --- Section 3: Heatmap ---
            st.markdown("#### 3. Activity Heatmap (曜日別活動量)")
            hm_d = analytics_data.get('heatmap', [])
            if hm_d:
                hm_df = pd.DataFrame(hm_d)
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                chart_heat = alt.Chart(hm_df).mark_rect().encode(
                    x=alt.X('time_zone', title='区分 (現在Dayのみ)'),
                    y=alt.Y('weekday', sort=days_order, title='曜日'),
                    color=alt.Color('count', title='完了数', scale=alt.Scale(scheme='orangered')),
                    tooltip=['weekday', 'count']
                ).properties(height=300)
                st.altair_chart(chart_heat, use_container_width=True)
                st.caption("※時間データがないため、曜日ごとの総量で表示しています。")

            # --- Section 4: Trend ---
            st.markdown("#### 4. Efficiency Trend (週次 平均保有日数)")
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
