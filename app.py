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

# --- スマートテキスト解析 (改善版) ---
def extract_serials_with_date(text, default_date):
    """
    あらゆる形式のテキストから8桁の数字と、それに関連する日付を抽出する
    """
    results = []
    default_date_str = default_date.strftime('%Y-%m-%d')
    
    # 行ごとに分割して解析
    lines = text.split('\n')
    
    # 一時バッファ
    current_date = default_date_str
    
    # 日付パターン (YYYY-MM-DD, YYYY/MM/DD)
    date_pattern = re.compile(r'(\d{4})[-/](\d{2})[-/](\d{2})')
    # シリアルパターン (8桁数字)
    serial_pattern = re.compile(r'\b(\d{8})\b')
    
    # 全文から「シリアル」と「日付」のペアを探す
    # SpotJobsの形式: "シリアル..." の行と "保有時間..." の行がセットになっていることが多い
    # あるいはブロックごとに分かれている
    
    # ブロック分割アプローチ（空行区切り）
    blocks = text.split('\n\n')
    if len(blocks) < 2: # 改行が少ない場合は行ベースで見る
        blocks = lines

    for block in blocks:
        # ブロック内の日付を探す
        d_match = date_pattern.search(block)
        if d_match:
            # 正規化した日付文字列
            block_date = f"{d_match.group(1)}-{d_match.group(2)}-{d_match.group(3)}"
        else:
            block_date = default_date_str
            
        # ブロック内のシリアルを探す
        serials = serial_pattern.findall(block)
        for s in serials:
            results.append((s, block_date))
            
    # 重複排除（同じ番号なら、日付が特定できている方を優先したいが、ここでは単純に後勝ち）
    unique_map = {r[0]: r[1] for r in results}
    return list(unique_map.items())

def extract_serials_only(text):
    return list(set(re.findall(r'\b\d{8}\b', text)))

# --- データ取得 ---
def get_database():
    """databaseシートから全データを取得"""
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        try:
            sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            # シート作成を試みる
            try:
                wb = client.open('battery_db')
                sheet = wb.add_worksheet(title=NEW_SHEET_NAME, rows=1000, cols=10)
                sheet.append_row(EXPECTED_HEADERS)
            except:
                st.error(f"シート '{NEW_SHEET_NAME}' が見つかりません。作成も失敗しました。")
                return pd.DataFrame()

        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # データが空、またはヘッダーだけの状態
        if df.empty:
            return pd.DataFrame(columns=EXPECTED_HEADERS)
        
        # 型変換
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        if 'ステータス' in df.columns:
            df['ステータス'] = df['ステータス'].astype(str).str.strip()
        else:
            # ステータス列がない致命的エラーの修復
            st.warning("データ構造を自動修復しました（ステータス列の追加）")
            sheet.insert_row(EXPECTED_HEADERS, index=1)
            return pd.DataFrame(columns=EXPECTED_HEADERS)

        df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0).astype(int)
        
        for col in ['保有開始日', '完了日']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            
        return df
    except Exception as e:
        st.error(f"データ読込エラー: {e}")
        return pd.DataFrame()

def get_active_inventory(df_all):
    """ステータスが「在庫」のものだけ抽出"""
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

# --- 書き込み・更新系 ---
def register_new_inventory(data_list):
    """
    新規在庫を追加
    重複チェック：現在「在庫」ステータスのものとの重複のみ防ぐ。
    過去データ（補充済）との重複は許可する。
    """
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    
    # 現在の在庫を取得して重複チェック
    all_records = sheet.get_all_records()
    df = pd.DataFrame(all_records)
    
    current_inventory_serials = set()
    if not df.empty and 'ステータス' in df.columns:
        # 文字列として比較、空白削除
        active_df = df[df['ステータス'].astype(str).str.strip() == '在庫']
        current_inventory_serials = set(active_df['シリアルナンバー'].astype(str).tolist())
    
    # ヘッダーチェック
    headers = sheet.row_values(1)
    if not headers or headers != EXPECTED_HEADERS:
        if not headers: sheet.append_row(EXPECTED_HEADERS)
        else: sheet.insert_row(EXPECTED_HEADERS, index=1)

    rows_to_add = []
    skipped_count = 0
    
    for s, d in data_list:
        s_str = str(s)
        # 既に「在庫」にあるならスキップ
        if s_str in current_inventory_serials:
            skipped_count += 1
            continue
            
        d_str = d.strftime('%Y-%m-%d') if isinstance(d, (datetime.date, datetime.datetime)) else str(d)
        # シリアル, ステータス, 保有開始日, 完了日, エリア, 金額, 備考
        rows_to_add.append([s_str, '在庫', d_str, '', '', '', ''])
    
    if rows_to_add:
        sheet.append_rows(rows_to_add)
        
    return len(rows_to_add), skipped_count

def update_status_bulk(target_serials, new_status, complete_date=None, zone="", price=0, memo=""):
    """ステータス更新"""
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
    updated_count = 0
    target_set = set(str(s) for s in target_serials)
    
    comp_str = ""
    if complete_date:
        comp_str = complete_date.strftime('%Y-%m-%d') if isinstance(complete_date, (datetime.date, datetime.datetime)) else str(complete_date)

    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = str(row.get('ステータス', '')).strip()
        
        # 「在庫」のものだけ対象
        if st_val == '在庫' and s in target_set:
            r = i + 2
            cells.append(gspread.Cell(r, col_status, new_status))
            cells.append(gspread.Cell(r, col_end, comp_str))
            cells.append(gspread.Cell(r, col_zone, zone))
            cells.append(gspread.Cell(r, col_price, price))
            if memo: cells.append(gspread.Cell(r, col_memo, memo))
            updated_count += 1
            
    if cells: sheet.update_cells(cells)
    return updated_count

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
        c, bg, st_t = "#c62828", "#fff5f5", f"🔥 要返却 (残{p_days}日)"
        bd = "#e57373"
    elif days <= 3: 
        c, bg, st_t = "#2e7d32", "#f1f8e9", "💎 Bonus"
        bd = "#81c784"
    else: 
        c, bg, st_t = "#616161", "#ffffff", f"🐢 通常 (残{p_days}日)"
        bd = "#bdbdbd"
        
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
    st.set_page_config(page_title="Battery Manager V5", page_icon="⚡", layout="wide")
    st.markdown("<style>.stSlider{padding-top:1rem;}</style>", unsafe_allow_html=True)
    today = get_today_jst()

    # セッション
    if 'stocktake_buffer' not in st.session_state: st.session_state['stocktake_buffer'] = []
    if 'parsed_data' not in st.session_state: st.session_state['parsed_data'] = None

    # データ読込
    df_all = get_database()
    
    # 在庫・履歴
    if not df_all.empty and 'ステータス' in df_all.columns:
        df_inv = get_active_inventory(df_all)
        df_hist = df_all[df_all['ステータス'] != '在庫']
    else:
        df_inv = pd.DataFrame()
        df_hist = pd.DataFrame()

    # 集計
    week_earnings = 0
    week_count = 0
    if not df_hist.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        w_df = df_hist[(df_hist['完了日'] >= start_of_week) & (df_hist['ステータス'] == '補充済')]
        week_count = len(w_df)
        week_earnings = int(w_df['金額'].sum())
    
    cur_bonus = get_vol_bonus(week_count)

    # タブ
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏠 ホーム", "🔍 検索", "📦 在庫", "💰 収益", "📝 棚卸"])

    # 1. ホーム
    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬概算", f"¥ {week_earnings:,}")
        c2.metric("補充本数", f"{week_count} 本")
        c3.metric("現在ボーナス", f"+{cur_bonus}円")
        st.divider()

        mode = st.radio("モード", ["取出 (登録)", "補充 (確定)"], horizontal=True)
        
        if mode == "取出 (登録)":
            st.caption("SpotJobsアプリのリストをペーストして「読込」→「登録実行」してください。")
            txt = st.text_area("テキスト貼付", height=100)
            date_in = st.date_input("基準日 (読取不可時)", value=today)
            
            if st.button("読込 (内容確認)", icon=":material/search:"):
                if txt:
                    parsed = extract_serials_with_date(txt, date_in)
                    if parsed:
                        st.session_state['parsed_data'] = parsed
                        st.success(f"{len(parsed)} 件読み込みました")
                    else:
                        st.warning("数字が見つかりませんでした")
            
            if st.session_state['parsed_data']:
                st.dataframe(pd.DataFrame(st.session_state['parsed_data'], columns=["SN","日付"]), hide_index=True)
                if st.button("登録実行", type="primary"):
                    cnt, skip = register_new_inventory(st.session_state['parsed_data'])
                    msg = f"✅ {cnt}件 を在庫に登録しました"
                    if skip > 0: msg += f" (※ {skip}件は既に在庫にあるためスキップ)"
                    st.success(msg)
                    st.session_state['parsed_data'] = None
                    import time
                    time.sleep(1)
                    st.rerun()

        else: # 補充
            col_d, col_z = st.columns([1,1])
            date_done = col_d.date_input("補充日", value=today)
            zone = col_z.selectbox("エリア", ZONE_OPTIONS)
            txt = st.text_area("補充リスト貼付", height=100)
            
            if txt:
                sns = extract_serials_only(txt)
                if sns:
                    price = ZONES[zone] + get_vol_bonus(week_count + len(sns))
                    st.info(f"{len(sns)}件検出 / 単価 ¥{price}")
                    if st.button("補充確定", type="primary"):
                        cnt = update_status_bulk(sns, "補充済", date_done, zone, price)
                        if cnt > 0:
                            st.success(f"{cnt}件 更新しました")
                            import time
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("更新対象が見つかりません（既に補充済か、未登録）")

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
            term = str(sn_in)
            hits = df_all[df_all['シリアルナンバー'].str.endswith(term)]
            if not hits.empty:
                st.success(f"{len(hits)}件 ヒット")
                for _, row in hits.iterrows():
                    st.write(f"状態: **{row['ステータス']}** (SN: {row['シリアルナンバー']})")
            else:
                st.warning("なし")

    # 3. 在庫 (救済機能)
    with tab3:
        st.metric("在庫数", f"{len(df_inv)}")
        
        with st.expander("＋ リストから一括登録 (強制)"):
            st.caption("ここにSpotJobsのリストを貼れば、問答無用で「在庫」として登録します。")
            force_txt = st.text_area("リスト貼り付け")
            if st.button("在庫として登録する"):
                if force_txt:
                    parsed = extract_serials_with_date(force_txt, today)
                    cnt, skip = register_new_inventory(parsed)
                    st.success(f"{cnt}件 登録完了！")
                    import time
                    time.sleep(1)
                    st.rerun()

        st.dataframe(df_inv, use_container_width=True)

    # 4. 収益
    with tab4:
        st.metric("今週", f"¥{week_earnings:,}")
        if not df_hist.empty:
            df_g = df_hist[df_hist['ステータス']=='補充済']
            st.dataframe(df_g.sort_values('完了日', ascending=False), use_container_width=True)

    # 5. 棚卸
    with tab5:
        cur = st.session_state['stocktake_buffer']
        st.info(f"読込数: {len(cur)}")
        
        txt_stock = st.text_area("リスト追加")
        if st.button("追加"):
            if txt_stock:
                add = extract_serials_with_date(txt_stock, today)
                st.session_state['stocktake_buffer'].extend(add)
                uniq = {s:d for s,d in st.session_state['stocktake_buffer']}
                st.session_state['stocktake_buffer'] = list(uniq.items())
                st.rerun()
        
        if st.button("リセット"):
            st.session_state['stocktake_buffer'] = []
            st.rerun()
            
        st.divider()
        if st.button("照合開始", type="primary"):
            if cur:
                s_map = {s:d for s,d in cur}
                if not df_inv.empty:
                    db_map = dict(zip(df_inv['シリアルナンバー'], df_inv['保有開始日']))
                else:
                    db_map = {}
                
                def fdate(d): return d.strftime('%Y-%m-%d') if pd.notnull(d) else ""
                
                missing_db = []
                for s, d in s_map.items():
                    # 既に在庫にあればスキップ
                    if s not in db_map: missing_db.append((s, d))
                
                if missing_db:
                    st.warning(f"未登録: {len(missing_db)}件")
                    if st.button("一括登録"):
                        register_new_inventory(missing_db)
                        st.success("登録完了")
                        st.rerun()
                else:
                    st.success("すべて登録済みです")
            else:
                st.warning("リストを入力してください")

if __name__ == '__main__':
    main()
