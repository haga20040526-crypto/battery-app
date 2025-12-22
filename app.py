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
# 新しい運用ではこのシート1枚ですべて管理します
NEW_SHEET_NAME = 'database' 
# 移行用（旧シート名）
OLD_INV_SHEET = 'sheet1'
OLD_HIST_SHEET = 'history'

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
    # 後勝ちで重複排除
    unique_map = {r[0]: r[1] for r in results}
    return list(unique_map.items())

def extract_serials_only(text):
    return list(set(re.findall(r'\b\d{8}\b', text)))

# --- データ取得・ソート ---
def get_database():
    """databaseシートから全データを取得しDataFrame化"""
    client = get_connection()
    if not client: return pd.DataFrame()
    try:
        sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # カラムが存在しない場合の初期化
        expected_cols = ['シリアルナンバー', 'ステータス', '保有開始日', '完了日', 'エリア', '金額', '備考']
        if df.empty:
            return pd.DataFrame(columns=expected_cols)
        
        # 型変換
        df['シリアルナンバー'] = df['シリアルナンバー'].astype(str)
        df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0).astype(int)
        
        # 日付変換
        for col in ['保有開始日', '完了日']:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            
        return df
    except Exception as e:
        st.error(f"データベース読み込みエラー: {e}")
        return pd.DataFrame()

def get_active_inventory(df_all):
    """ステータスが「在庫」のものだけ抽出"""
    if df_all.empty: return df_all
    df = df_all[df_all['ステータス'] == '在庫'].copy()
    
    # カスタムソート: 日付(昇順) > 末尾番号(昇順)
    df['rev_serial'] = df['シリアルナンバー'].apply(lambda x: x[::-1])
    df_sorted = df.sort_values(by=['保有開始日', 'rev_serial'], ascending=[True, True])
    df_sorted = df_sorted.drop(columns=['rev_serial'])
    return df_sorted

def get_history_data(df_all):
    """ステータスが「補充済」「不明」「手動」などの履歴データを抽出"""
    if df_all.empty: return df_all
    # 在庫以外 = 履歴
    return df_all[df_all['ステータス'] != '在庫'].copy()

def get_vol_bonus(count):
    if count >= 150: return 20
    elif count >= 100: return 15
    elif count >= 50: return 10
    elif count >= 20: return 5
    else: return 0

# --- ★重要: ステータス更新ロジック (行削除なし) ---
def update_status_bulk(target_serials, new_status, complete_date, zone="", price=0, memo=""):
    """
    対象シリアルのうち、現在「在庫」になっている行を探し、
    ステータス・完了日・金額などを上書き更新する。
    """
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    
    # 全データ取得
    all_records = sheet.get_all_records()
    
    # 更新用バッチリスト
    cells_to_update = []
    updated_count = 0
    
    # カラム位置の特定 (1始まり)
    headers = sheet.row_values(1)
    try:
        col_status = headers.index('ステータス') + 1
        col_end_date = headers.index('完了日') + 1
        col_zone = headers.index('エリア') + 1
        col_price = headers.index('金額') + 1
        col_memo = headers.index('備考') + 1
        col_serial = headers.index('シリアルナンバー') + 1
    except ValueError:
        st.error("シートのヘッダーが見つかりません。A1行目を確認してください。")
        return 0

    target_set = set(str(s) for s in target_serials)
    
    # 行ごとにチェック
    for i, row in enumerate(all_records):
        # 行番号 (データは2行目から、iは0始まりなので +2)
        row_num = i + 2
        s_num = str(row.get('シリアルナンバー', ''))
        current_status = row.get('ステータス', '')
        
        # 「在庫」かつ「対象リストに含まれる」場合のみ更新
        if current_status == '在庫' and s_num in target_set:
            # gspreadのCellオブジェクトを作成してリストに追加
            cells_to_update.append(gspread.Cell(row_num, col_status, new_status))
            cells_to_update.append(gspread.Cell(row_num, col_end_date, str(complete_date)))
            cells_to_update.append(gspread.Cell(row_num, col_zone, zone))
            cells_to_update.append(gspread.Cell(row_num, col_price, price))
            if memo:
                cells_to_update.append(gspread.Cell(row_num, col_memo, memo))
            
            updated_count += 1
            # 同じ番号が複数回リストにあっても、1回処理したらセットから外す（重複処理防止）
            # ただし、同じシリアルが在庫に複数ある場合は（異常データだが）全て処理される
    
    if cells_to_update:
        sheet.update_cells(cells_to_update)
        
    return updated_count

def register_new_inventory(data_list):
    """新規在庫を追加 (行追加)"""
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    
    # data_list = [(serial, date_str), ...]
    rows = []
    for s, d in data_list:
        # シリアル, ステータス, 保有開始日, 完了日, エリア, 金額, 備考
        rows.append([str(s), '在庫', str(d), '', '', '', ''])
    
    if rows:
        sheet.append_rows(rows)
    return len(rows)

def update_dates_bulk(updates_list):
    """日付のみ修正 (在庫のものに限る)"""
    client = get_connection()
    sheet = client.open('battery_db').worksheet(NEW_SHEET_NAME)
    all_records = sheet.get_all_records()
    headers = sheet.row_values(1)
    col_start_date = headers.index('保有開始日') + 1
    
    cells_to_update = []
    updates_map = {str(s): d for s, d in updates_list}
    
    count = 0
    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        st_val = row.get('ステータス', '')
        
        if st_val == '在庫' and s in updates_map:
            row_num = i + 2
            new_d = updates_map[s]
            cells_to_update.append(gspread.Cell(row_num, col_start_date, str(new_d)))
            count += 1
            
    if cells_to_update:
        sheet.update_cells(cells_to_update)
    return count

# --- データ移行ツール ---
def migrate_old_data():
    """旧シート(sheet1, history)からデータをdatabaseに移行する"""
    client = get_connection()
    wb = client.open('battery_db')
    new_sheet = wb.worksheet(NEW_SHEET_NAME)
    
    # 既存データ確認 (誤爆防止)
    if len(new_sheet.get_all_values()) > 1:
        st.error("databaseシートに既にデータがあります。移行は空のシートでのみ可能です。")
        return

    rows_to_add = []
    
    # 1. 在庫データ移行
    try:
        inv_sheet = wb.worksheet(OLD_INV_SHEET)
        inv_data = inv_sheet.get_all_records()
        for row in inv_data:
            s = str(row.get('シリアルナンバー', ''))
            d = row.get('保有開始日', '')
            if s:
                rows_to_add.append([s, '在庫', d, '', '', '', '旧データ移行'])
    except:
        st.warning("旧在庫シートが見つかりませんでした。")

    # 2. 履歴データ移行
    try:
        hist_sheet = wb.worksheet(OLD_HIST_SHEET)
        hist_data = hist_sheet.get_all_records()
        for row in hist_data:
            s = str(row.get('シリアルナンバー', ''))
            start_d = row.get('保有開始日', '')
            end_d = row.get('補充日', '')
            zone = row.get('補充エリア', '')
            price = row.get('確定報酬額', 0)
            memo = row.get('備考', '')
            
            # ステータス判定
            status = '補充済'
            if '棚卸' in memo or '不明' in memo:
                status = '不明'
            
            if s:
                rows_to_add.append([s, status, start_d, end_d, zone, price, memo])
    except:
        st.warning("旧履歴シートが見つかりませんでした。")
        
    if rows_to_add:
        # 1000行ずつバッチ追加 (タイムアウト防止)
        chunk_size = 1000
        for i in range(0, len(rows_to_add), chunk_size):
            new_sheet.append_rows(rows_to_add[i:i+chunk_size])
        st.success(f"移行完了: {len(rows_to_add)} 件のデータを移動しました。")
    else:
        st.info("移行するデータがありませんでした。")


# --- カード表示 (1行HTML) ---
def create_inventory_card_html(row, today):
    # active_inventoryのDataFrameは保有開始日がdate型になっている前提
    start_date = row['保有開始日']
    if pd.isnull(start_date):
        start_date_str = "-"
        days_held = 0
        p_days = 99
    else:
        start_date_str = start_date.strftime('%m/%d')
        days_held = (today - start_date).days
        p_days = PENALTY_LIMIT_DAYS - days_held
    
    serial = row['シリアルナンバー']
    last4 = serial[-4:] if len(serial) >= 4 else serial
    
    if p_days <= 5: priority = 1
    elif days_held <= 3: priority = 2
    else: priority = 3

    if priority == 1:
        border, text_c, status, bg_c = "#e57373", "#c62828", f"🔥 要返却 (残{p_days}日)", "#fff5f5"
    elif priority == 2:
        border, text_c, status, bg_c = "#81c784", "#2e7d32", "💎 Bonus", "#f1f8e9"
    else:
        border, text_c, status, bg_c = "#bdbdbd", "#616161", f"🐢 通常 (残{p_days}日)", "#ffffff"
    
    return textwrap.dedent(f"""<div style="background-color: {bg_c}; border-radius: 8px; border-left: 8px solid {border}; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 12px; margin-bottom: 12px;"><div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;"><div style="font-size: 12px; font-weight: bold; color: {text_c};">{status}</div><div style="font-size: 12px; font-weight: bold; color: #555;">{start_date_str}〜</div></div><div style="font-size: 34px; font-weight: 900; color: #212121; line-height: 1.1; letter-spacing: 1px;">{last4}</div><div style="text-align: right; font-size: 10px; color: #999; font-family: monospace;">{serial}</div></div>""")

def create_search_card_html(row, today):
    start_date = row['保有開始日']
    if pd.isnull(start_date):
        start_date_str = "-"
        days_held = 0
    else:
        start_date_str = start_date.strftime('%Y-%m-%d')
        days_held = (today - start_date).days
        
    serial = row['シリアルナンバー']
    
    return textwrap.dedent(f"""<div style="background-color: #ffffff; border-radius: 12px; border: 1px solid #e0e0e0; padding: 15px; margin-bottom: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);"><div style="font-size: 13px; color: #757575; margin-bottom: 4px;">保管開始日</div><div style="font-size: 42px; font-weight: 900; color: #212121; line-height: 1.1; letter-spacing: 1px;">{start_date_str}</div><div style="font-size: 18px; font-weight: bold; color: #424242; margin-top: 8px; background-color: #f5f5f5; display: inline-block; padding: 4px 12px; border-radius: 20px;">経過 {days_held}日目</div><div style="font-size: 12px; color: #bdbdbd; margin-top: 15px; padding-top: 8px; border-top: 1px solid #f0f0f0; font-family: monospace; text-align: right;">SN: {serial}</div></div>""")


# --- メイン処理 ---
def main():
    st.set_page_config(page_title="Battery Manager V2", page_icon="⚡", layout="wide")
    st.markdown("""<style>.stSlider { padding-top: 1rem; }</style>""", unsafe_allow_html=True)
    today = get_today_jst()

    # セッション初期化
    if 'parsed_data' not in st.session_state: st.session_state['parsed_data'] = None
    if 'stocktake_buffer' not in st.session_state: st.session_state['stocktake_buffer'] = []

    # サイドバー：データ移行用
    with st.sidebar:
        st.header("⚙️ 管理メニュー")
        with st.expander("旧データ移行ツール"):
            st.warning("注意: 'database'シートが空である必要があります。")
            if st.button("旧データから移行を実行"):
                with st.spinner("データ移行中..."):
                    migrate_old_data()

    # データ読み込み (1枚のシートから全て取得)
    df_all = get_database()
    
    # 在庫と履歴に分割
    df_inv = get_active_inventory(df_all)
    df_hist = get_history_data(df_all)

    # 集計計算 (今週分)
    week_earnings = 0
    week_count = 0
    total_earnings = 0
    
    if not df_hist.empty:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        # 今週の完了分
        weekly_df = df_hist[(df_hist['完了日'] >= start_of_week) & (df_hist['ステータス'] == '補充済')]
        week_count = len(weekly_df)
        week_earnings = weekly_df['金額'].sum()
        
        # 全期間の補充済
        total_earnings = df_hist[df_hist['ステータス'] == '補充済']['金額'].sum()

    current_bonus = get_vol_bonus(week_count)

    # --- タブ ---
    tab_home, tab_search, tab_inventory, tab_history, tab_stocktake = st.tabs(["🏠 ホーム", "🔍 個別検索", "📦 在庫", "💰 収益", "📝 棚卸し"])

    # 1. ホーム
    with tab_home:
        st.markdown("### 今週の成果")
        c1, c2, c3 = st.columns(3)
        c1.metric("報酬概算", f"¥ {week_earnings:,}")
        c2.metric("補充本数", f"{week_count} 本")
        if current_bonus < 20:
            next_target = 20 if week_count < 20 else (50 if week_count < 50 else (100 if week_count < 100 else 150))
            remain = next_target - week_count
            c3.metric("現在ボーナス", f"+{current_bonus}円", delta=f"あと{remain}本")
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
                    else: st.warning("シリアルナンバーが見つかりませんでした")
            if st.session_state['parsed_data']:
                st.divider()
                st.markdown("##### 以下の内容で登録しますか？")
                preview_df = pd.DataFrame(st.session_state['parsed_data'], columns=["シリアルナンバー", "取得日"])
                st.dataframe(preview_df, hide_index=True, use_container_width=True)
                
                col_reg, col_cancel = st.columns([1, 1])
                with col_reg:
                    if st.button("登録を実行する", type="primary", use_container_width=True, icon=":material/save:"):
                        with st.spinner('登録中...'):
                            # 既存チェック: 同じシリアルが「在庫」にあればスキップすべきだが、
                            # ユーザー要望により「再度取出」もあり得るので、単純に追加する (ステータス管理で区別)
                            added = register_new_inventory(st.session_state['parsed_data'])
                        st.success(f"✅ {added} 件を登録しました")
                        st.session_state['parsed_data'] = None
                        import time
                        time.sleep(2)
                        st.rerun()
                with col_cancel:
                    if st.button("キャンセル"):
                        st.session_state['parsed_data'] = None
                        st.rerun()

        elif job_mode == "補充 (報酬確定)":
            st.caption("対象の在庫ステータスを「補充済」に更新します。")
            col_date, col_area = st.columns([1, 1])
            with col_date: target_date = st.date_input("補充日", value=today)
            with col_area:
                default_index = ZONE_OPTIONS.index("D: その他 (船橋など)")
                selected_zone_name = st.selectbox("エリア選択", ZONE_OPTIONS, index=default_index)
            input_text = st.text_area("テキスト貼付", height=100, placeholder="ここにペースト...")
            
            if input_text:
                extracted = extract_serials_only(input_text)
                if extracted:
                    st.info(f"{len(extracted)} 件を検出しました")
                    base_price = ZONES[selected_zone_name]
                    # 現在の週本数 + 今回の本数でボーナス計算
                    est_bonus = get_vol_bonus(week_count + len(extracted))
                    est_total_price = base_price + est_bonus
                    st.metric("適用単価", f"¥{est_total_price}", f"基準{base_price}+ボ{est_bonus}")
                    
                    if st.button("補充を確定する", type="primary", use_container_width=True, icon=":material/check_circle:"):
                        with st.spinner('ステータス更新中...'):
                            # 金額計算込みで更新
                            updated_count = update_status_bulk(
                                extracted, 
                                "補充済", 
                                target_date, 
                                zone=selected_zone_name, 
                                price=est_total_price
                            )
                        if updated_count > 0:
                            st.success(f"✅ {updated_count} 件の補充を確定しました")
                            import time
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("エラー: 更新対象が見つかりませんでした（既に補充済か、未登録です）")

        st.divider()
        col_title, col_slider = st.columns([2, 1])
        with col_title: st.subheader("ピックアップ推奨")
        with col_slider: display_count = st.slider("表示数", 1, 20, 7)
        
        if not df_inv.empty:
            df_rec = df_inv.copy()
            df_rec['days_held'] = df_rec['保有開始日'].apply(lambda x: (today - x).days)
            df_rec['penalty_left'] = PENALTY_LIMIT_DAYS - df_rec['days_held']
            def get_rank(r):
                if r['penalty_left'] <= 5: return 1 
                elif r['days_held'] <= 3: return 2
                return 3
            df_rec['rank'] = df_rec.apply(get_rank, axis=1)
            # ランク順 > 日数順
            df_rec_sorted = df_rec.sort_values(['rank', 'days_held'], ascending=[True, False])
            
            top_n = df_rec_sorted.head(display_count)
            # 末尾ソート(見やすさ)
            top_n['rev_serial'] = top_n['シリアルナンバー'].apply(lambda x: x[::-1])
            top_n = top_n.sort_values(by=['保有開始日', 'rev_serial'])
            
            if not top_n.empty:
                for i in range(0, len(top_n), 4):
                    chunk = top_n.iloc[i:i+4]
                    cols = st.columns(4)
                    for idx, (_, row) in enumerate(chunk.iterrows()):
                        with cols[idx]:
                            st.markdown(create_inventory_card_html(row, today), unsafe_allow_html=True)
            else: st.info("表示対象なし")

    # 2. 検索
    with tab_search:
        st.markdown("### 🔢 個別バッテリー検索")
        search_num = st.number_input("シリアルナンバー (下4桁)", min_value=0, value=0, step=1, format="%d")
        if search_num > 0 and not df_all.empty:
            search_term = str(int(search_num)) 
            # 全データから検索
            hits = df_all[df_all['シリアルナンバー'].str.endswith(search_term)]
            st.divider()
            if not hits.empty:
                st.success(f"{len(hits)} 件ヒット")
                for _, row in hits.iterrows():
                    st.write(f"ステータス: **{row['ステータス']}**")
                    st.markdown(create_search_card_html(row, today), unsafe_allow_html=True)
            else:
                st.warning("データが見つかりません")

    # 3. 在庫一覧
    with tab_inventory:
        st.subheader("📦 在庫詳細")
        if not df_inv.empty:
            st.metric("現在の在庫総数", f"{len(df_inv)} 本")
            
            with st.expander("🗑️ 在庫から削除 (手動修正)"):
                del_serial = st.text_input("削除(不明扱いに変更)するシリアル")
                if st.button("削除実行"):
                    cnt = update_status_bulk([del_serial], "手動削除", today, memo="手動削除")
                    if cnt: 
                        st.success("ステータスを削除に変更しました")
                        st.rerun()
            
            st.divider()
            df_disp = df_inv.copy()
            df_disp['保有開始日'] = df_disp['保有開始日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            st.dataframe(df_disp[['シリアルナンバー', '保有開始日', 'ステータス']], use_container_width=True, hide_index=True)
        else:
            st.info("在庫はありません")

    # 4. 収益
    with tab_history:
        st.markdown("### 📊 収益レポート")
        c_m, c_s = st.columns([3, 1])
        c_m.metric("今週の確定報酬", f"¥ {week_earnings:,}")
        c_s.metric("全期間積算", f"¥ {total_earnings:,}")
        
        st.divider()
        if not df_hist.empty:
            # 補充済のものだけグラフ化
            df_graph = df_hist[df_hist['ステータス'] == '補充済'].copy()
            if not df_graph.empty:
                chart_df = df_graph.groupby('完了日')['金額'].sum().reset_index()
                chart_df.columns = ['日付', '金額']
                chart = alt.Chart(chart_df).mark_bar(color='#29b6f6').encode(
                    x=alt.X('日付:T', axis=alt.Axis(format='%m/%d')),
                    y='金額:Q',
                    tooltip=['日付', '金額']
                ).interactive()
                st.altair_chart(chart, use_container_width=True)

        st.markdown("#### 履歴一覧")
        if not df_hist.empty:
            df_hist_disp = df_hist.sort_values('完了日', ascending=False)
            st.dataframe(df_hist_disp, use_container_width=True)

    # 5. 棚卸し
    with tab_stocktake:
        st.markdown("### 📝 在庫棚卸し")
        current_buffer = st.session_state['stocktake_buffer']
        st.info(f"読込済み: {len(current_buffer)} 本")
        
        with st.expander("データの追加読込", expanded=True):
            stock_input = st.text_area("リスト貼り付け (分割可)", height=100)
            c_add, c_clear = st.columns([1, 1])
            with c_add:
                if st.button("リストに追加", type="primary"):
                    if stock_input:
                        new_items = extract_serials_with_date(stock_input, today)
                        st.session_state['stocktake_buffer'].extend(new_items)
                        # 重複排除
                        unique = {s: d for s, d in st.session_state['stocktake_buffer']}
                        st.session_state['stocktake_buffer'] = list(unique.items())
                        st.rerun()
            with c_clear:
                if st.button("リセット"):
                    st.session_state['stocktake_buffer'] = []
                    st.rerun()

        if st.button("照合開始", type="primary", use_container_width=True):
            if not df_inv.empty and current_buffer:
                stock_map = {s: d for s, d in current_buffer}
                db_map = dict(zip(df_inv['シリアルナンバー'], df_inv['保有開始日']))
                
                def fmt(d): return d.strftime('%Y-%m-%d') if not pd.isnull(d) else ""
                
                missing_in_db = []
                missing_in_hand = []
                date_mismatch = []
                
                # A: 手元にあるがDBにない
                for s, d in stock_map.items():
                    if s not in db_map:
                        missing_in_db.append((s, d))
                    else:
                        # C: 日付違い
                        if fmt(db_map[s]) != d: # d is string
                            date_mismatch.append((s, d, fmt(db_map[s])))
                
                # B: DBにあるが手元にない
                for s in db_map.keys():
                    if s not in stock_map:
                        missing_in_hand.append(s)
                
                if not any([missing_in_db, missing_in_hand, date_mismatch]):
                    st.success("🎉 ズレはありません！")
                else:
                    if missing_in_db:
                        st.warning(f"🚨 未登録: {len(missing_in_db)} 件")
                        if st.button("一括登録する"):
                            cnt = register_new_inventory(missing_in_db)
                            st.success(f"{cnt}件 登録しました")
                            st.rerun()
                    
                    if missing_in_hand:
                        st.error(f"⚠️ 手元なし: {len(missing_in_hand)} 件")
                        with st.expander("詳細"):
                            st.write(", ".join(missing_in_hand))
                            if st.button("棚卸削除 (ステータス更新)"):
                                cnt = update_status_bulk(missing_in_hand, "不明", today, memo="棚卸削除")
                                st.success(f"{cnt}件 削除済にしました")
                                st.rerun()
                    
                    if date_mismatch:
                        st.info(f"📅 日付ズレ: {len(date_mismatch)} 件")
                        if st.button("日付を更新"):
                            updates = [(i[0], i[1]) for i in date_mismatch]
                            cnt = update_dates_bulk(updates)
                            st.success(f"{cnt}件 更新しました")
                            st.rerun()

if __name__ == '__main__':
    main()
