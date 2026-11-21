from flask import Flask, request, send_from_directory, jsonify, Response
from flask_cors import CORS
import paho.mqtt.client as mqtt
import json
import os
import sqlite3
import csv
import io
import ssl
import threading
import queue as Q
import time
import requests
import qrcode
from datetime import datetime

app = Flask(__name__)
CORS(app)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')
DB_PATH     = os.path.join(os.path.dirname(__file__), 'orders.db')

def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                flavor            TEXT    NOT NULL,
                sweetness_level   TEXT    NOT NULL,
                sweetness_percent INTEGER NOT NULL,
                price             REAL    NOT NULL DEFAULT 45.0,
                order_status      TEXT    NOT NULL DEFAULT 'completed',
                ordered_at        TEXT    NOT NULL
            )
        ''')
        # Migrate existing DB — add columns if they don't exist yet
        for col, definition in [('price', 'REAL NOT NULL DEFAULT 45.0'),
                                 ('order_status', "TEXT NOT NULL DEFAULT 'completed'")]:
            try:
                con.execute(f'ALTER TABLE orders ADD COLUMN {col} {definition}')
            except sqlite3.OperationalError:
                pass  # column already exists
        con.commit()

init_db()

DEFAULTS = {
    "mqtt": {"broker": "broker.hivemq.com", "port": 1883, "username": "", "password": ""},
    "topics": {
        "order":     "watereiei/orderstester",
        "machine":   "watereiei/machine/dispense",
        "drink":     "watereiei/machine/drink_selected",
        "sweetness": "watereiei/machine/sweetness_selected",
        "reset":     "watereiei/machine/reset",
    },
    "sweetness_map":  {"หวานน้อย": 50, "หวานปกติ": 100, "หวานมาก": 150},
    "menus": [
        {"name": "Blue Syrup",  "icon": "🔵", "color": "#007bff", "pump_channel": 1, "price": 45.0,
         "keywords": ["blue", "น้ำเงิน", "ฟ้า", "บลู"], "locked": True},
        {"name": "Red Syrup",   "icon": "🔴", "color": "#b52b2b", "pump_channel": 2, "price": 45.0,
         "keywords": ["red", "แดง", "เรด"], "locked": True},
        {"name": "Green Syrup", "icon": "🟢", "color": "#28a745", "pump_channel": 3, "price": 45.0,
         "keywords": ["green", "เขียว", "กรีน"], "locked": True},
    ],
    "payment": {"promptpay_id": "", "slipok_api_key": "", "slipok_branch_id": ""},
}

def load_config():
    cfg = json.loads(json.dumps(DEFAULTS))  # deep copy so callers can't mutate DEFAULTS
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            for key, value in saved.items():
                if isinstance(value, dict) and isinstance(cfg.get(key), dict):
                    cfg[key].update(value)
                else:
                    cfg[key] = value
        except Exception as e:
            print(f"Config load error, using defaults: {e}")
    return cfg

cfg = load_config()
MQTT_BROKER          = cfg["mqtt"]["broker"]
MQTT_PORT            = cfg["mqtt"]["port"]
MQTT_USERNAME        = cfg["mqtt"].get("username", "")
MQTT_PASSWORD        = cfg["mqtt"].get("password", "")
MQTT_ORDER_TOPIC     = cfg["topics"]["order"]
MQTT_MACHINE_TOPIC   = cfg["topics"]["machine"]
MQTT_DRINK_TOPIC     = cfg["topics"]["drink"]
MQTT_SWEETNESS_TOPIC = cfg["topics"]["sweetness"]
MQTT_RESET_TOPIC     = cfg["topics"].get("reset", "watereiei/machine/reset")
SWEETNESS_MAP        = cfg["sweetness_map"]
MENUS                = cfg["menus"]
DRINK_PUMP_MAP       = {m["name"]: m["pump_channel"] for m in MENUS}
PRICE_MAP            = {m["name"]: m["price"] for m in MENUS}

PAYMENT_CFG          = cfg.get("payment", {})
PROMPTPAY_ID         = PAYMENT_CFG.get("promptpay_id", "")
SLIPOK_API_KEY       = PAYMENT_CFG.get("slipok_api_key", "")
SLIPOK_BRANCH_ID     = PAYMENT_CFG.get("slipok_branch_id", "")

# ─── Dispense queue ──────────────────────────────────────────────────────────
DISPENSE_TOTAL_SECS = 14   # 10s dispense + 3s done + 1s buffer
dispense_queue = Q.Queue()

def _dispense_worker():
    while True:
        job = dispense_queue.get()
        try:
            print(f"[Queue] Processing: {job['machine_cmd']['drink']} (remaining: {dispense_queue.qsize()})")
            _publish(MQTT_ORDER_TOPIC,   job['order_data'])
            _publish(MQTT_MACHINE_TOPIC, job['machine_cmd'])
            time.sleep(DISPENSE_TOTAL_SECS)
        except Exception as e:
            print(f"[Queue] Error: {e}")
        finally:
            dispense_queue.task_done()

threading.Thread(target=_dispense_worker, daemon=True).start()


def _publish(topic, payload):
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_USERNAME:
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        if MQTT_PORT == 8883:
            client.tls_set(tls_version=ssl.PROTOCOL_TLS)
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_start()
        result = client.publish(topic, json.dumps(payload, ensure_ascii=False))
        result.wait_for_publish(timeout=5)
        client.loop_stop()
        client.disconnect()
        print(f"MQTT published to {topic}")
        return True
    except Exception as e:
        print(f"MQTT error on {topic}: {e}")
        return False

# ─── PromptPay QR + SlipOK verification ──────────────────────────────────────
SLIPOK_VERIFY_URL = "https://api.slipok.com/api/line/apikey/{}"


def _tlv(id_, value):
    return f"{id_}{len(value):02d}{value}"


def _crc16_ccitt(payload):
    crc = 0xFFFF
    for byte in payload.encode('ascii'):
        crc ^= (byte << 8)
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return format(crc, '04X')


def _promptpay_target(promptpay_id):
    digits = ''.join(c for c in promptpay_id if c.isdigit())
    if len(digits) >= 15:
        return digits, '03'          # e-Wallet ID
    if len(digits) >= 13:
        return digits, '02'          # National ID / Tax ID
    if digits.startswith('0'):
        digits = digits[1:]
    return '0066' + digits, '01'     # Mobile number (MSISDN)


def generate_promptpay_payload(promptpay_id, amount):
    target, target_type = _promptpay_target(promptpay_id)
    merchant_info = _tlv('00', 'A000000677010111') + _tlv(target_type, target)
    parts = [
        _tlv('00', '01'),   # Payload Format Indicator
        _tlv('01', '12'),   # Point of Initiation Method: dynamic (has amount)
        _tlv('29', merchant_info),
        _tlv('53', '764'),  # Currency: THB
        _tlv('54', f'{amount:.2f}'),
        _tlv('58', 'TH'),
    ]
    payload = ''.join(parts) + '6304'
    return payload + _crc16_ccitt(payload)


def verify_slip_with_slipok(expected_amount, file=None, qr_data=None):
    if not SLIPOK_API_KEY or not SLIPOK_BRANCH_ID:
        return False, None, "ยังไม่ได้ตั้งค่า SlipOK API (branch id / api key)"

    url     = SLIPOK_VERIFY_URL.format(SLIPOK_BRANCH_ID)
    headers = {"x-authorization": SLIPOK_API_KEY}

    try:
        if file is not None:
            files = {'files': (file.filename, file.stream, file.mimetype)}
            data  = {'log': 'true'}
            resp  = requests.post(url, headers=headers, files=files, data=data, timeout=20)
        else:
            data = {'data': qr_data, 'log': 'true'}
            resp = requests.post(url, headers=headers, data=data, timeout=20)
        result = resp.json()
    except Exception as e:
        return False, None, f"เชื่อมต่อ SlipOK ไม่สำเร็จ: {e}"

    slip = result.get('data') or {}
    if not result.get('success'):
        return False, slip, slip.get('message') or result.get('message') or 'ตรวจสอบสลิปไม่ผ่าน'

    slip_amount = float(slip.get('amount', 0) or 0)
    if abs(slip_amount - float(expected_amount)) > 0.01:
        return False, slip, f"ยอดเงินไม่ตรง (สลิป ฿{slip_amount:.2f} / ต้องชำระ ฿{expected_amount:.2f})"

    return True, slip, None


def send_to_mqtt(order_data):
    menu      = order_data.get('menu', '')
    sweetness = order_data.get('sweetness', '')
    timestamp = order_data.get('timestamp', '')

    machine_cmd = {
        "command":           "DISPENSE",
        "drink":             menu,
        "pump_channel":      DRINK_PUMP_MAP.get(menu, 0),
        "sweetness_level":   sweetness,
        "sweetness_percent": SWEETNESS_MAP.get(sweetness, 100),
        "timestamp":         timestamp,
    }
    dispense_queue.put({'order_data': order_data, 'machine_cmd': machine_cmd})
    print(f"[Queue] Order queued (queue size: {dispense_queue.qsize()})")
    return True


# --- Order endpoints ---

@app.route('/api/save-order', methods=['POST'])
def save_order():
    # Creates the order in a "pending_payment" state. The machine is only
    # queued to dispense once the slip has been verified — see /api/verify-slip.
    order_data = request.json
    print(f"ได้รับออเดอร์: {order_data}")

    flavor   = order_data.get('menu', '')
    sweet    = order_data.get('sweetness', '')
    pct      = SWEETNESS_MAP.get(sweet, 0)
    price    = PRICE_MAP.get(flavor, 45.0)
    ts       = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute(
            'INSERT INTO orders (flavor, sweetness_level, sweetness_percent, price, order_status, ordered_at) VALUES (?,?,?,?,?,?)',
            (flavor, sweet, pct, price, 'pending_payment', ts)
        )
        order_id = cur.lastrowid
        con.commit()

    return jsonify({"status": "success", "id": order_id, "price": price})


@app.route('/api/qr/<int:order_id>', methods=['GET'])
def get_payment_qr(order_id):
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        row = con.execute('SELECT price FROM orders WHERE id = ?', (order_id,)).fetchone()

    if row is None:
        return jsonify({"status": "error", "message": "ไม่พบออเดอร์นี้"}), 404
    if not PROMPTPAY_ID:
        return jsonify({"status": "error", "message": "ยังไม่ได้ตั้งค่า PromptPay ID"}), 400

    payload = generate_promptpay_payload(PROMPTPAY_ID, row['price'])
    img = qrcode.make(payload)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return Response(buf.getvalue(), mimetype='image/png')


@app.route('/api/verify-slip', methods=['POST'])
def verify_slip():
    order_id = request.form.get('order_id')
    if not order_id and request.is_json:
        order_id = (request.json or {}).get('order_id')
    if not order_id:
        return jsonify({"status": "error", "message": "missing order_id"}), 400

    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        order = con.execute('SELECT * FROM orders WHERE id = ?', (int(order_id),)).fetchone()

    if order is None:
        return jsonify({"status": "error", "message": "ไม่พบออเดอร์นี้"}), 404
    if order['order_status'] == 'completed':
        return jsonify({"status": "error", "message": "ออเดอร์นี้ชำระเงินแล้ว"}), 400
    if order['order_status'] == 'cancelled':
        return jsonify({"status": "error", "message": "ออเดอร์นี้ถูกยกเลิกแล้ว"}), 400

    slip_file = request.files.get('file')
    qr_data   = None
    if slip_file is None:
        qr_data = request.form.get('data')
        if not qr_data and request.is_json:
            qr_data = (request.json or {}).get('data')

    if slip_file is None and not qr_data:
        return jsonify({"status": "error", "message": "ไม่พบไฟล์สลิปหรือข้อมูล QR"}), 400

    verified, slip, error = verify_slip_with_slipok(order['price'], file=slip_file, qr_data=qr_data)
    if not verified:
        return jsonify({"status": "error", "message": error}), 400

    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE orders SET order_status = 'completed' WHERE id = ?", (order['id'],))
        con.commit()

    send_to_mqtt({
        'menu':      order['flavor'],
        'sweetness': order['sweetness_level'],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })

    return jsonify({
        "status":   "success",
        "transRef": slip.get('transRef') if slip else None,
        "amount":   slip.get('amount') if slip else order['price'],
    })


@app.route('/api/orders', methods=['GET'])
def get_orders():
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute('SELECT * FROM orders ORDER BY id DESC').fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/orders/export', methods=['GET'])
def export_orders():
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute('SELECT * FROM orders ORDER BY id ASC').fetchall()

    buf = io.StringIO()
    buf.write('﻿')  # UTF-8 BOM — Excel needs this to read Thai correctly
    writer = csv.writer(buf)
    writer.writerow(['id', 'flavor', 'sweetness_level', 'price', 'order_status', 'ordered_at'])
    for r in rows:
        writer.writerow([r['id'], r['flavor'], r['sweetness_level'],
                         r['price'], r['order_status'], f'="{r["ordered_at"]}"'])

    filename = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        buf.getvalue().encode('utf-8'),
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@app.route('/api/orders/<int:order_id>', methods=['DELETE'])
def delete_order(order_id):
    with sqlite3.connect(DB_PATH) as con:
        con.execute('DELETE FROM orders WHERE id = ?', (order_id,))
        con.commit()
    return jsonify({"status": "ok"})


@app.route('/api/orders/reset', methods=['DELETE'])
def reset_orders():
    with sqlite3.connect(DB_PATH) as con:
        con.execute('DELETE FROM orders')
        con.execute("DELETE FROM sqlite_sequence WHERE name='orders'")
        con.commit()
    return jsonify({"status": "ok"})


@app.route('/api/orders/<int:order_id>/cancel', methods=['POST'])
def cancel_order(order_id):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE orders SET order_status = 'cancelled' WHERE id = ?", (order_id,))
        con.commit()
    return jsonify({"status": "ok"})


@app.route('/api/reset', methods=['POST'])
def reset_machine():
    threading.Thread(target=_publish, args=(MQTT_RESET_TOPIC, {"command": "RESET"}), daemon=True).start()
    return jsonify({"status": "ok"})


@app.route('/api/queue', methods=['GET'])
def get_queue():
    size = dispense_queue.qsize()
    return jsonify({"queue_size": size, "wait_seconds": size * DISPENSE_TOTAL_SECS})


@app.route('/api/test-order', methods=['POST'])
def test_order():
    # Admin-only testing helper (called from the settings page) — skips payment
    # verification entirely and drives the real MQTT dispense pipeline directly,
    # so pump/MQTT wiring can be checked after editing menus.
    data     = request.json or {}
    flavor   = data.get('menu', '')
    sweet    = data.get('sweetness', 'หวานปกติ')

    if flavor not in DRINK_PUMP_MAP:
        return jsonify({"status": "error", "message": "ไม่พบเมนูนี้"}), 404

    pct   = SWEETNESS_MAP.get(sweet, 100)
    price = PRICE_MAP.get(flavor, 0)
    ts    = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            'INSERT INTO orders (flavor, sweetness_level, sweetness_percent, price, order_status, ordered_at) VALUES (?,?,?,?,?,?)',
            (flavor, sweet, pct, price, 'test', ts)
        )
        con.commit()

    send_to_mqtt({'menu': flavor, 'sweetness': sweet, 'timestamp': ts})

    return jsonify({
        "status":      "success",
        "queue_size":  dispense_queue.qsize(),
        "wait_seconds": dispense_queue.qsize() * DISPENSE_TOTAL_SECS,
    })


@app.route('/api/select-drink', methods=['POST'])
def select_drink():
    data = request.json
    menu = data.get('menu', '')
    payload = {
        "event":        "DRINK_SELECTED",
        "drink":        menu,
        "pump_channel": DRINK_PUMP_MAP.get(menu, 0),
        "timestamp":    data.get('timestamp', ''),
    }
    print(f"เลือกเครื่องดื่ม: {payload}")
    threading.Thread(target=_publish, args=(MQTT_DRINK_TOPIC, payload), daemon=True).start()
    return jsonify({"status": "ok"})


@app.route('/api/select-sweetness', methods=['POST'])
def select_sweetness():
    data      = request.json
    sweetness = data.get('sweetness', '')
    payload = {
        "event":             "SWEETNESS_SELECTED",
        "sweetness_level":   sweetness,
        "sweetness_percent": SWEETNESS_MAP.get(sweetness, 100),
        "timestamp":         data.get('timestamp', ''),
    }
    print(f"เลือกความหวาน: {payload}")
    threading.Thread(target=_publish, args=(MQTT_SWEETNESS_TOPIC, payload), daemon=True).start()
    return jsonify({"status": "ok"})


def _parse_menus(raw_menus):
    if not isinstance(raw_menus, list) or not raw_menus:
        raise ValueError("menus must be a non-empty list")

    seen  = set()
    menus = []
    for m in raw_menus:
        name = str(m["name"]).strip()
        if not name or name in seen:
            raise ValueError(f"invalid or duplicate menu name: {name!r}")
        seen.add(name)
        menus.append({
            "name":         name,
            "icon":         str(m.get("icon", "🥤")),
            "color":        str(m.get("color", "#8b4513")),
            "pump_channel": int(m["pump_channel"]),
            "price":        float(m["price"]),
            "keywords":     [str(k).strip() for k in m.get("keywords", []) if str(k).strip()],
            "locked":       bool(m.get("locked", False)),
        })
    return menus


# --- Config endpoints ---

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify(load_config())


@app.route('/api/menus', methods=['GET'])
def get_menus():
    # Public-safe subset for kiosk pages — no MQTT/SlipOK credentials.
    return jsonify([
        {"name": m["name"], "icon": m["icon"], "color": m["color"],
         "price": m["price"], "keywords": m.get("keywords", []), "locked": m.get("locked", False)}
        for m in MENUS
    ])


@app.route('/api/config', methods=['POST'])
def save_config():
    global MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD
    global MQTT_ORDER_TOPIC, MQTT_MACHINE_TOPIC, MQTT_DRINK_TOPIC, MQTT_SWEETNESS_TOPIC
    global SWEETNESS_MAP, MENUS, DRINK_PUMP_MAP, PRICE_MAP
    global PROMPTPAY_ID, SLIPOK_API_KEY, SLIPOK_BRANCH_ID

    data = request.json
    try:
        new_cfg = {
            "mqtt": {
                "broker":   str(data["mqtt"]["broker"]).strip(),
                "port":     int(data["mqtt"]["port"]),
                "username": str(data["mqtt"].get("username", "")).strip(),
                "password": str(data["mqtt"].get("password", "")).strip(),
            },
            "topics": {
                "order":     str(data["topics"]["order"]).strip(),
                "machine":   str(data["topics"]["machine"]).strip(),
                "drink":     str(data["topics"]["drink"]).strip(),
                "sweetness": str(data["topics"]["sweetness"]).strip(),
            },
            "sweetness_map": {
                "หวานน้อย":  int(data["sweetness_map"]["หวานน้อย"]),
                "หวานปกติ": int(data["sweetness_map"]["หวานปกติ"]),
                "หวานมาก":  int(data["sweetness_map"]["หวานมาก"]),
            },
            "menus": _parse_menus(data.get("menus")),
            "payment": {
                "promptpay_id":     str(data.get("payment", {}).get("promptpay_id", "")).strip(),
                "slipok_api_key":   str(data.get("payment", {}).get("slipok_api_key", "")).strip(),
                "slipok_branch_id": str(data.get("payment", {}).get("slipok_branch_id", "")).strip(),
            },
        }
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(new_cfg, f, ensure_ascii=False, indent=2)

    MQTT_BROKER          = new_cfg["mqtt"]["broker"]
    MQTT_PORT            = new_cfg["mqtt"]["port"]
    MQTT_USERNAME        = new_cfg["mqtt"]["username"]
    MQTT_PASSWORD        = new_cfg["mqtt"]["password"]
    MQTT_ORDER_TOPIC     = new_cfg["topics"]["order"]
    MQTT_MACHINE_TOPIC   = new_cfg["topics"]["machine"]
    MQTT_DRINK_TOPIC     = new_cfg["topics"]["drink"]
    MQTT_SWEETNESS_TOPIC = new_cfg["topics"]["sweetness"]
    SWEETNESS_MAP        = new_cfg["sweetness_map"]
    MENUS                = new_cfg["menus"]
    DRINK_PUMP_MAP       = {m["name"]: m["pump_channel"] for m in MENUS}
    PRICE_MAP            = {m["name"]: m["price"] for m in MENUS}
    PROMPTPAY_ID         = new_cfg["payment"]["promptpay_id"]
    SLIPOK_API_KEY       = new_cfg["payment"]["slipok_api_key"]
    SLIPOK_BRANCH_ID     = new_cfg["payment"]["slipok_branch_id"]

    print(f"Config saved: broker={MQTT_BROKER}:{MQTT_PORT}")
    return jsonify({"status": "ok"})


@app.route('/api/config/test', methods=['POST'])
def test_connection():
    data     = request.json
    broker   = str(data.get("broker", "")).strip()
    port     = int(data.get("port", 1883))
    username = str(data.get("username", "")).strip()
    password = str(data.get("password", "")).strip()
    try:
        client = mqtt.Client()
        if username:
            client.username_pw_set(username, password)
        if port == 8883:
            client.tls_set(tls_version=ssl.PROTOCOL_TLS)
        client.connect(broker, port, keepalive=5)
        client.disconnect()
        return jsonify({"status": "ok", "message": f"เชื่อมต่อสำเร็จ ({broker}:{port})"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


# --- Static file serving ---

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def static_files(path):
    return send_from_directory('.', path)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)