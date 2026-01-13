import threading
import datetime
import re
import time
import sys
from evdev import ecodes
from flask import Flask, render_template, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
from gspread.cell import Cell
import config 

app = Flask(__name__)

# --- GLOBAL STATE ---
latest_scan = {
    "timestamp": 0,
    "data": {"box": "READY", "serial": "---", "msg": "Waiting...", "status": "wait"}
}
current_mode = "COLLECT" 
current_target_date = datetime.date.today().strftime('%Y-%m-%d')

# --- Helper Logic ---
def normalize_input(text):
    if not text: return ""
    table = str.maketrans('０１２３４５６７８９', '0123456789')
    return text.translate(table).strip()

def parse_serial_from_scan(text):
    text = normalize_input(text)
    if "b=" in text:
        try:
            return re.search(r'(\d{8})', text.split("b=")[-1]).group(1)
        except: pass
    match = re.search(r'\b(\d{8})\b', text)
    return match.group(1) if match else None

def generate_job_id():
    """
    J + 日時(YYMMDDHHMMSS) のジョブIDを生成
    例: J260113153000
    """
    return datetime.datetime.now().strftime("J%y%m%d%H%M%S")

# --- DB Helper ---
def get_sheet():
    try:
        creds = Credentials.from_service_account_file("creds.json", scopes=config.SCOPES)
        client = gspread.authorize(creds)
        return client.open(config.SHEET_NAME).worksheet(config.WORKSHEET)
    except Exception as e:
        print(f"DB Error: {e}")
        return None

def get_current_box_status(records):
    """現在のレコードからBOXの使用状況（日付・個数）を計算"""
    box_usage = {f"BOX-{i}": {'date': None, 'count': 0} for i in range(1, config.TOTAL_BOXES + 1)}
    
    for row in records:
        st = str(row.get('ステータス', ''))
        box = str(row.get('備考', '')).upper()
        
        # ステータスが「在庫」で、かつBOXに入っているものをカウント
        if st == '在庫' and box.startswith('BOX-'):
            if box in box_usage:
                box_usage[box]['count'] += 1
                if box_usage[box]['date'] is None:
                    d_str = str(row.get('保有開始日', ''))
                    if d_str: box_usage[box]['date'] = d_str
    return box_usage

# --- Core Logic ---

def run_bulk_allocation():
    """【格納モード突入時】場所が決まっていない在庫（持出中扱い）を一括でBOXに割り当てる"""
    print(">> STARTING BULK ALLOCATION...", flush=True)
    sheet = get_sheet()
    if not sheet: return
    
    records = sheet.get_all_records()
    box_usage = get_current_box_status(records)
    
    # 割り当てが必要な行を探す（ステータス='在庫' だが BOXが空のもの）
    targets = []
    for i, row in enumerate(records):
        st = str(row.get('ステータス', ''))
        box = str(row.get('備考', '')).upper()
        date_str = str(row.get('保有開始日', ''))
        
        # BOXが空 ＝ 「現場から戻ってきた」または「未格納」
        if st == '在庫' and (not box.startswith('BOX-')):
            targets.append({
                'row_idx': i + 2, # スプレッドシートは2行目から
                'date': date_str
            })
    
    if not targets:
        print(">> NO TARGETS FOR ALLOCATION.", flush=True)
        return

    targets.sort(key=lambda x: x['date'])
    cells_to_update = []
    
    for t in targets:
        target_box = None
        t_date = t['date']
        
        # Rule A: 同じ日付のBOXを探す
        for b_id in range(1, config.TOTAL_BOXES + 1):
            b_name = f"BOX-{b_id}"
            info = box_usage[b_name]
            if info['date'] == t_date and info['count'] < config.BOX_CAPACITY:
                target_box = b_name
                break
        
        # Rule B: 空っぽのBOXを探す
        if not target_box:
            for b_id in range(1, config.TOTAL_BOXES + 1):
                b_name = f"BOX-{b_id}"
                info = box_usage[b_name]
                if info['count'] == 0:
                    target_box = b_name
                    box_usage[b_name]['date'] = t_date 
                    break
        
        if target_box:
            box_usage[target_box]['count'] += 1
            cells_to_update.append(Cell(t['row_idx'], 7, target_box))
            print(f"Allocating Row {t['row_idx']} ({t_date}) -> {target_box}", flush=True)
        else:
            print(f"Row {t['row_idx']} -> FULL (No space)", flush=True)

    if cells_to_update:
        try:
            sheet.update_cells(cells_to_update)
            print(f">> BULK UPDATE COMPLETE: {len(cells_to_update)} items.", flush=True)
        except Exception as e:
            print(f"BULK UPDATE FAILED: {e}", flush=True)

def process_spotjobs_text(text):
    """【回収モード】テキスト一括登録（一回の貼り付けにつき1つのJob IDを発行）"""
    sheet = get_sheet()
    if not sheet: return {"status": "error", "msg": "DB Error"}

    matches = re.findall(r'シリアルナンバー[:\s]+(\d{8}).*?保有時間[:\s]+(\d{4}-\d{2}-\d{2})', text, re.DOTALL)
    if not matches: return {"status": "error", "msg": "No data found"}

    # ★ 今回のバッチ処理用のIDを発行
    batch_job_id = generate_job_id()

    all_records = sheet.get_all_records()
    serial_map = {}
    for i, row in enumerate(all_records):
        s = str(row.get('シリアルナンバー', ''))
        if s: serial_map[s] = i + 2

    rows_to_append = []
    count_new = 0
    count_update = 0
    
    for serial, date_str in matches:
        if serial in serial_map:
            # 既存更新
            r = serial_map[serial]
            try:
                sheet.update_cell(r, 2, "在庫")
                sheet.update_cell(r, 3, date_str)
                sheet.update_cell(r, 7, "") # BOXを空に
                sheet.update_cell(r, 8, batch_job_id) # ★Job ID更新 (H列)
                count_update += 1
            except: pass
        else:
            # 新規登録 (H列にJob ID)
            rows_to_append.append([str(serial), "在庫", date_str, "", "", "", "", batch_job_id])
            count_new += 1
    
    if rows_to_append: sheet.append_rows(rows_to_append)
    
    return {
        "status": "success", 
        "msg": f"Registered: {len(matches)}", 
        "detail": f"New:{count_new} Upd:{count_update}",
        "job_id": batch_job_id
    }

def process_storage_scan(serial):
    """【格納モード】割り当てられたBOXを表示"""
    sheet = get_sheet()
    try:
        cell = sheet.find(str(serial))
        if not cell:
            return {"status": "error", "msg": "NOT REGISTERED", "box": "ERR", "serial": serial}
        
        row_vals = sheet.row_values(cell.row)
        current_box = row_vals[6] if len(row_vals) > 6 else "" 
        
        if current_box.startswith("BOX-"):
             return {"status": "success", "msg": "ALLOCATED", "box": current_box, "serial": serial}
        else:
            return {"status": "error", "msg": "NO BOX ASSIGNED", "box": "FULL?", "serial": serial}

    except Exception as e:
        return {"status": "error", "msg": str(e), "box": "ERR", "serial": serial}

def register_out(serial):
    """【出庫モード】BOX指定を削除して空にする"""
    sheet = get_sheet()
    try:
        cell = sheet.find(str(serial))
        if cell:
            # BOX列(7)を空にする -> 持出中扱い
            sheet.update_cell(cell.row, 7, "") 
            return {"status": "success", "msg": "DEPLOYED", "box": "---", "serial": serial}
    except: pass
    return {"status": "error", "msg": "NOT FOUND", "box": "???", "serial": serial}

# --- API Routes ---
@app.route('/')
def index():
    return render_template('mobile_live.html', default_date=current_target_date, mode=current_mode, zones=config.ZONES.keys())

@app.route('/api/status_poll')
def status_poll():
    return jsonify({"mode": current_mode, "latest": latest_scan})

@app.route('/api/update_settings', methods=['POST'])
def update_settings():
    global current_mode, current_target_date
    data = request.json
    new_mode = data.get('mode', current_mode)
    
    # STOREモードに入った瞬間、一括割り当てを実行
    if new_mode == "STORE" and current_mode != "STORE":
        run_bulk_allocation()
        
    current_mode = new_mode
    if 'date' in data: current_target_date = data['date']
    return jsonify({"status": "ok"})

@app.route('/api/register_spotjobs', methods=['POST'])
def register_spotjobs():
    text = request.json.get('text', '')
    return jsonify(process_spotjobs_text(text))

# --- HARDWARE SCANNER SERVICE ---
def scanner_listener():
    global latest_scan
    print(">> HARDWARE SCANNER SERVICE STARTED", flush=True)
    device = None
    target_device_name = "NT USB Keyboard"

    scancodes = {
        2: "1", 3: "2", 4: "3", 5: "4", 6: "5", 7: "6", 8: "7", 9: "8", 10: "9", 11: "0",
        12: "-", 13: "=", 16: "q", 17: "w", 18: "e", 19: "r", 20: "t", 21: "y", 22: "u", 23: "i", 24: "o", 25: "p",
        30: "a", 31: "s", 32: "d", 33: "f", 34: "g", 35: "h", 36: "j", 37: "k", 38: "l", 44: "z", 45: "x", 46: "c", 
        47: "v", 48: "b", 49: "n", 50: "m", 51: ",", 52: ".", 53: "/"
    }
    shift_map = {"1": "!", "2": "@", "-": "_", "=": "+", "/": "?"}

    while True:
        try:
            if device is None:
                devices = [evdev.InputDevice(path) for path in evdev.list_devices()]
                for dev in devices:
                    if target_device_name in dev.name:
                        device = dev
                        print(f">> CONNECTED TO: {dev.name}", flush=True)
                        device.grab()
                        break
                if device is None:
                    time.sleep(2)
                    continue

            current_code = ""
            is_shift = False
            
            for event in device.read_loop():
                if event.type == ecodes.EV_KEY:
                    if event.code in [42, 54]: 
                        is_shift = (event.value == 1 or event.value == 2)
                        continue
                    
                    if event.value == 1: 
                        if event.code == 28: # Enter
                            if current_code:
                                serial = parse_serial_from_scan(current_code)
                                if serial:
                                    print(f"Scanned: {serial} (Mode: {current_mode})", flush=True)
                                    res = {}
                                    if current_mode == "COLLECT":
                                        # ★スキャナーからの登録はブロックする（テキスト貼り付けのみ許可）
                                        res = {"status": "warning", "msg": "Please use TEXT PASTE", "box": "USE APP", "serial": serial}
                                    elif current_mode == "STORE": 
                                        res = process_storage_scan(serial)
                                    elif current_mode == "OUT": 
                                        res = register_out(serial)
                                    latest_scan = {"timestamp": time.time(), "data": res}
                                current_code = ""
                        elif event.code in scancodes:
                            char = scancodes[event.code]
                            if is_shift and char in shift_map: char = shift_map[char]
                            elif is_shift: char = char.upper()
                            current_code += char
        except Exception as e:
            print(f"Scanner Error: {e}", flush=True)
            device = None
            time.sleep(3)

if __name__ == '__main__':
    t = threading.Thread(target=scanner_listener, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
