# -*- coding: utf-8 -*-
import os, json, math
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ========= تنظیمات =========
SPREADSHEET_ID = "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes"
ALL_DATA_SHEET = "All_Data"
DAILY_SHEET    = "Daily_Performance"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TASK_TYPES = [
    "Receive","Locate","Sort","Pack_Multi","Pack_Single",
    "Pick_Small","Presort_Small","Stock taking","Pick_Larg","Presort_Larg",
]

# ========= اتصال =========
def _client():
    if "GOOGLE_CREDENTIALS" in os.environ:
        creds = Credentials.from_service_account_info(
            json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES
        )
    else:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)

# ========= کمک‌تابع‌ها: تاریخ =========
def serial_to_datetime(n):
    base = datetime(1899, 12, 30)
    return base + timedelta(days=float(n))

def parse_date_floor(v):
    """B1/C1 را (هر فرمتی: سریال، YYYY-MM-DD، MM/DD/YYYY، …) به datetime در ساعت 00:00 تبدیل می‌کند."""
    if v in (None, ""): return None
    try:
        f = float(v)
        return serial_to_datetime(f).replace(hour=0, minute=0, second=0, microsecond=0)
    except:
        pass
    s = str(v).strip()
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%Y/%m/%d","%d/%m/%Y","%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(hour=0, minute=0, second=0, microsecond=0)
        except:
            continue
    try:
        return datetime.fromisoformat(s).replace(hour=0, minute=0, second=0, microsecond=0)
    except:
        return None

# ========= کمک‌تابع‌ها: عدد/درصد =========
PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
ARABIC_DIGITS  = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def normalize_digits(s: str) -> str:
    return s.translate(PERSIAN_DIGITS).translate(ARABIC_DIGITS)

def to_number_locale(x, default=0.0):
    if x in (None, ""): return default
    s = normalize_digits(str(x)).replace("\u00a0"," ").strip()
    s = s.replace("%","").replace("٪","").replace(" ","").replace("٫",".")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".","").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        if "," in s:
            parts = s.split(",")
            s = s.replace(",", ".") if len(parts[-1]) in (1,2) else s.replace(",", "")
    try:
        return float(s)
    except:
        return default

def to_percent_locale(x, default=None):
    """58.9% → 0.589 ، ۱۳۱٫۵٪ → 1.315 ، 0.78 → 0.78"""
    val = to_number_locale(x, default=None)
    if val is None: return default
    if 0 <= val <= 1: return val
    if 1 < val <= 1000: return val/100.0
    return default

# ========= A1 =========
def a1(col_idx, row_idx):
    s, c = "", col_idx
    while c:
        c, r = divmod(c-1, 26); s = chr(65+r) + s
    return f"{s}{row_idx}"

# ========= بدنه =========
def build_daily_performance():
    gc = _client()
    ss = gc.open_by_key(SPREADSHEET_ID)
    ws_daily = ss.worksheet(DAILY_SHEET)
    ws_all   = ss.worksheet(ALL_DATA_SHEET)

    # --- فیلترها (طبق اسکرین‌شات: B1, C1, E1) ---
    B1 = ws_daily.acell("B1").value   # Start
    C1 = ws_daily.acell("C1").value   # End
    E1 = ws_daily.acell("E1").value   # Shift یا All/Total

    start_dt = parse_date_floor(B1)
    end_dt   = parse_date_floor(C1)
    if start_dt is None or end_dt is None:
        ws_daily.update(range_name="A3", values=[["⚠️ تاریخ‌های B1/C1 معتبر نیستند."]])
        return
    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)

    sel_raw = (str(E1).strip() if E1 not in (None,"") else "")
    selected_shift = None if sel_raw.lower() in {"","all","total","total_daily","total-daily","جمع","کل"} else sel_raw

    # --- خواندن All_Data ---
    values = ws_all.get_all_values()
    if len(values) < 2:
        ws_daily.update(range_name="A3", values=[["⚠️ All_Data خالی است."]])
        return
    headers, rows = values[0], values[1:]

    def idx(names):
        for n in names:
            if n in headers: return headers.index(n)
        return -1

    c_full  = idx(["full_name","Full_Name","FULL_NAME"])
    c_task  = idx(["task_type","Task_Type","TASK_TYPE"])
    c_qty   = idx(["quantity","Quantity","QUANTITY"])
    c_occ   = idx(["occupied_hours","Occupied_Hours","OCCUPIED_HOURS"])
    c_neg   = idx(["Negative_Minutes","negative_minutes","NEGATIVE_MINUTES"])
    c_p0    = idx(["performance_without_rotation"])
    c_p1    = idx(["performance_with_rotation"])
    c_date  = idx(["date","Date","DATE"])
    c_shift = idx(["Shift","shift","SHIFT"])

    if any(i<0 for i in [c_full,c_task,c_qty,c_occ,c_neg,c_p0,c_p1,c_date,c_shift]):
        ws_daily.update(range_name="A3", values=[["⚠️ ستون‌های لازم در All_Data یافت نشد."]])
        return

    # --- تجمیع ---
    summary  = {}  # name -> {quantity, occupied, negative, p0(list), p1(list)}
    detailed = {}  # name -> task -> همان ساختار

    for r in rows:
        d0 = parse_date_floor(r[c_date])
        if d0 is None: continue
        if not (start_dt <= d0 <= end_dt): continue
        if selected_shift is not None and str(r[c_shift]).strip() != selected_shift:
            continue

        name = r[c_full]
        task = str(r[c_task]).strip()
        if task == "Pack":   # فقط Pack_Multi/Single
            continue

        qty = to_number_locale(r[c_qty], 0.0)
        occ = to_number_locale(r[c_occ], 0.0)
        neg = to_number_locale(r[c_neg], 0.0)
        p0  = to_percent_locale(r[c_p0], default=None)
        p1  = to_percent_locale(r[c_p1], default=None)

        if name not in summary:
            summary[name] = {"quantity":0.0,"occupied":0.0,"negative":0.0,"p0":[],"p1":[]}
        summary[name]["quantity"] += qty
        summary[name]["occupied"] += occ
        summary[name]["negative"] += neg
        if p0 is not None: summary[name]["p0"].append(p0)
        if p1 is not None: summary[name]["p1"].append(p1)

        if name not in detailed:
            detailed[name] = {t: {"quantity":0.0,"occupied":0.0,"negative":0.0,"p0":[],"p1":[]} for t in TASK_TYPES}
        if task in TASK_TYPES:
            d = detailed[name][task]
            d["quantity"] += qty
            d["occupied"] += occ
            d["negative"] += neg
            if p0 is not None: d["p0"].append(p0)
            if p1 is not None: d["p1"].append(p1)

    # --- پاکسازی خروجی قبلی ---
    ws_daily.batch_clear(["A3:ZZ1000"])

    if not summary:
        ws_daily.update(range_name="A3", values=[[
            f"ℹ️ نتیجه فیلتر خالی است. Start={B1}  End={C1}  Shift={sel_raw or '(خالی)'}"
        ]])
        return

    # --- جدول 1: Summary ---
    t1_header = ["full_name","quantity","occupied_hours","Negative_Minutes","performance_without_rotation","performance_with_rotation"]
    t1_rows = []
    for name, s in summary.items():
        p0_avg = (sum(s["p0"])/len(s["p0"])) if s["p0"] else None
        p1_avg = (sum(s["p1"])/len(s["p1"])) if s["p1"] else None
        t1_rows.append([name, s["quantity"] or "", s["occupied"] or "", s["negative"] or "", p0_avg if p0_avg is not None else "", p1_avg if p1_avg is not None else ""])
    t1_rows.sort(key=lambda r: (r[5] if isinstance(r[5], (int,float)) else -1), reverse=True)

    table1 = [t1_header] + t1_rows
    ws_daily.update(range_name=f"A3:{a1(len(t1_header), 3+len(table1)-1)}", values=table1)

    # --- جدول 2: Detailed بر اساس ترتیب جدول 1 ---
    t2_header = ["full_name"]
    for t in TASK_TYPES:
        t2_header += [
            f"{t}_quantity", f"{t}_occupied_hours", f"{t}_Negative_Minutes",
            f"{t}_performance_without_rotation", f"{t}_performance_with_rotation"
        ]

    t2_rows = []
    for r in t1_rows:
        name = r[0]
        out = [name]
        per_name = detailed.get(name, {t: {"quantity":0.0,"occupied":0.0,"negative":0.0,"p0":[],"p1":[]} for t in TASK_TYPES})
        for t in TASK_TYPES:
            d = per_name[t]
            all_zero = (d["quantity"]==0 and d["occupied"]==0 and d["negative"]==0 and not d["p0"] and not d["p1"])
            if all_zero:
                out += ["","","","",""]
            else:
                p0_avg = (sum(d["p0"])/len(d["p0"])) if d["p0"] else None
                p1_avg = (sum(d["p1"])/len(d["p1"])) if d["p1"] else None
                out += [
                    d["quantity"] or "",
                    d["occupied"] or "",
                    d["negative"] or "",
                    p0_avg if p0_avg is not None else "",
                    p1_avg if p1_avg is not None else "",
                ]
        t2_rows.append(out)

    start_col_t2 = len(t1_header) + 2
    ws_daily.update(
        range_name=f"{a1(start_col_t2, 3)}:{a1(start_col_t2 + len(t2_header)-1, 3+len(t2_rows))}",
        values=[t2_header] + t2_rows
    )

    # --- فرمت درصدها ---
    end_row1 = 3 + len(table1) - 1
    ss.batch_update({
        "requests": [
            {   # جدول 1 → ستون‌های 5 و 6
                "repeatCell": {
                    "range": {
                        "sheetId": ws_daily.id,
                        "startRowIndex": 2, "endRowIndex": end_row1,
                        "startColumnIndex": 4, "endColumnIndex": 6
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]
    })

    end_row2 = 3 + len(t2_rows)
    requests = []
    for b in range(len(TASK_TYPES)):
        # در هر بلوک 5 ستون: qty, occ, neg, p0, p1  → دو تای آخر درصدند
        start_col_idx = (start_col_t2-1) + 1 + b*5
        for c in (start_col_idx+3, start_col_idx+4):
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws_daily.id,
                        "startRowIndex": 2, "endRowIndex": end_row2,
                        "startColumnIndex": c-1, "endColumnIndex": c
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })
    if requests:
        ss.batch_update({"requests": requests})

    print("✅ build_daily_performance: Done.")

# اجرای مستقیم
if __name__ == "__main__":
    build_daily_performance()

