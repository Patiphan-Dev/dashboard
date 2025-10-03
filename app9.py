import os
import uuid
from datetime import datetime, date
import pytz
import streamlit as st
from streamlit_calendar import calendar
import io, zipfile
import pandas as pd
from sqlalchemy import text # ใช้สำหรับ DML ใน PostgreSQL
import requests # 🆕 Import requests แทน supabase-py

# ====== IMPORT ANALYZERS ======
from CPU_Analyzer import CPU_Analyzer
from FAN_Analyzer import FAN_Analyzer
from MSU_Analyzer import MSU_Analyzer
from Line_Analyzer import Line_Analyzer
from Client_Analyzer import Client_Analyzer
from Fiberflapping_Analyzer import FiberflappingAnalyzer
from EOL_Core_Analyzer import EOLAnalyzer, CoreAnalyzer

from table1 import SummaryTableReport


# ====== CONFIG ======
st.set_page_config(layout="wide")
pd.set_option("styler.render.max_elements", 1_200_000)

# UPLOAD_DIR และ DB_FILE เดิมถูกยกเลิกการใช้งานแล้ว
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ====== DB INIT / CONNECTION & SUPABASE CLIENT INIT ======

# 1. SQL Connection (สำหรับ PostgreSQL Metadata Table)
# ใช้ st.connection เพื่อจัดการการเชื่อมต่อกับ Supabase PostgreSQL
try:
    conn = st.connection("supabase", type="sql")
except Exception as e:
    st.error(f"Failed to connect to Supabase SQL. Error: {e}")
    conn = None 

# 2. Supabase Client (สำหรับ Storage - ใช้ Requests แทน)
try:
    # 🆕 ดึงค่าจาก Streamlit Secrets และเตรียม URL/Headers
    SUPABASE_URL = st.secrets.supabase_client.url.rstrip('/')
    SUPABASE_KEY = st.secrets.supabase_client.anon_key
    BUCKET_NAME = st.secrets.supabase_client.bucket_name
    
    # สร้าง BASE URL สำหรับเรียก API (storage.v1.object)
    STORAGE_API_URL = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}" 

    # สร้าง Headers สำหรับการส่ง requests
    HEADERS = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "x-upsert": "true" # อนุญาตให้อัปโหลดทับได้
    }

except AttributeError:
    st.error("Please configure [supabase_client] secrets correctly.")
    SUPABASE_KEY, BUCKET_NAME, STORAGE_API_URL, HEADERS = None, None, None, None

# -----------------------------------------------------------


# ====== DB/STORAGE FUNCTIONS (Cloud Persistence - ใช้ Requests) ======

def save_file_to_storage(upload_date: str, file):
    """บันทึกไฟล์ลง Supabase Storage และ Metadata ลง PostgreSQL ด้วย Requests"""
    if conn is None or STORAGE_API_URL is None:
        st.warning("Cannot save file: Supabase connection or client is not available.")
        return

    # 1. บันทึกไฟล์ลง Supabase Storage (Upload)
    file_id = str(uuid.uuid4())
    stored_name = f"{file_id}_{file.name}"
    storage_path = f"{upload_date}/{stored_name}"

    upload_headers = HEADERS.copy()
    upload_headers["Content-Type"] = "application/zip" # กำหนด Content-Type เฉพาะ Upload

    try:
        # 🆕 POST request ไปยัง Supabase Storage API
        response = requests.post(
            f"{STORAGE_API_URL}/{storage_path}", 
            headers=upload_headers,
            data=file.getbuffer()
        )
        response.raise_for_status() # ตรวจสอบ HTTP Errors
        
    except requests.exceptions.RequestException as e:
        st.error(f"Error uploading file '{file.name}' to Supabase Storage: {e}")
        return

    # 2. บันทึก Metadata ลง PostgreSQL 
    current_time_str = datetime.now(pytz.timezone("Asia/Bangkok")).isoformat()
    with conn.session as session:
        session.execute(
            text(
                """
                INSERT INTO uploads (upload_date, orig_filename, stored_path, created_at)
                VALUES (:upload_date, :orig_filename, :stored_path, :created_at)
                """
            ), 
            params={
                "upload_date": upload_date, 
                "orig_filename": file.name, 
                "stored_path": storage_path, 
                "created_at": current_time_str
            }
        )
        session.commit()
        
@st.cache_data(ttl="1h")
def list_files_by_date(upload_date: str):
    """ดึงรายการไฟล์ทั้งหมดตามวันที่จาก PostgreSQL (Metadata)"""
    if conn is None: return []
    df = conn.query(
        "SELECT id, orig_filename, stored_path FROM uploads WHERE upload_date = :upload_date ORDER BY created_at DESC", 
        params={"upload_date": upload_date},
        ttl="1h"
    )
    # คืนค่าเป็น Tuple list เพื่อให้เข้ากับ Logic เดิม
    return list(df[['id', 'orig_filename', 'stored_path']].itertuples(index=False, name=None))


@st.cache_data(ttl=600)
def get_file_bytes_from_storage(storage_path: str):
    """ดึงไฟล์ bytes จาก Supabase Storage ด้วย Requests"""
    if STORAGE_API_URL is None:
        return None
    try:
        # 🆕 GET request เพื่อดาวน์โหลดไฟล์
        response = requests.get(
            f"{STORAGE_API_URL}/{storage_path}", 
            headers={"Authorization": f"Bearer {SUPABASE_KEY}"} # ใช้ Headers สำหรับ Auth เท่านั้น
        )
        response.raise_for_status()
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        st.error(f"Error downloading file from Storage: {e}")
        return None


def delete_file(file_id: int):
    """ลบไฟล์ออกจาก Storage และลบ Metadata ออกจาก PostgreSQL ด้วย Requests"""
    if conn is None or STORAGE_API_URL is None: return
        
    # 1. ดึง stored_path (Storage Path)
    df_path = conn.query("SELECT stored_path FROM uploads WHERE id = :id", params={"id": file_id}, ttl="1h")
    if df_path.empty: return
        
    storage_path = df_path['stored_path'].iloc[0]

    # 2. ลบไฟล์จาก Supabase Storage (Remove)
    try:
        # 🆕 DELETE request ไปยัง Supabase Storage API
        response = requests.delete(
            STORAGE_API_URL, 
            headers={"Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"},
            json={"prefixes": [storage_path]} 
        )
        response.raise_for_status()
        if response.status_code != 200:
             st.warning(f"Failed to delete file from Storage: HTTP {response.status_code}")
        
    except requests.exceptions.RequestException as e:
        st.warning(f"Failed to delete file from Storage: {e}. Metadata will still be removed.")

    # 3. ลบ metadata จาก PostgreSQL 
    with conn.session as session:
        session.execute(text("DELETE FROM uploads WHERE id = :id"), params={"id": file_id})
        session.commit()

@st.cache_data(ttl="1h")
def list_dates_with_files():
    """ดึงวันที่และจำนวนไฟล์ทั้งหมดจาก Supabase สำหรับ Calendar"""
    if conn is None: return []

    df = conn.query(
        "SELECT upload_date, COUNT(id) as count FROM uploads GROUP BY upload_date",
        ttl="1h"
    )
    # คืนค่าเป็น Tuple list เพื่อให้เข้ากับ Logic เดิม
    return list(df.itertuples(index=False, name=None))
# -----------------------------------------------------------


# ====== CLEAR SESSION & ZIP PARSER (ส่วนนี้คงเดิม) ======
def clear_all_uploaded_data():
    # ล้างสถานะการวิเคราะห์ทั้งหมด
    keys_to_clear = [k for k in st.session_state.keys() if k.endswith(("_data", "_file", "wason_log", "analyzer", "lb_pmap", "zip_loaded"))]
    for key in keys_to_clear:
        del st.session_state[key]
    st.session_state["zip_loaded"] = False
    
# ====== ZIP PARSER (คงเดิม) ======
KW = {
    "cpu": ("cpu",), "fan": ("fan",), "msu": ("msu",),
    "client": ("client", "client board"), "line": ("line","line board"),
    "wason": ("wason","log"), "osc": ("osc","osc optical"),
    "fm": ("fm","alarm","fault management"),
    "atten": ("optical attenuation report", "optical_attenuation_report", "optical attenuation"),
    "preset": ("mobaxterm", "moba xterm", "moba"),
}

LOADERS = {
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel,
    ".txt": lambda f: f.read().decode("utf-8", errors="ignore"),
}

def _ext(name: str) -> str:
    name = name.lower()
    return next((e for e in LOADERS if name.endswith(e)), "")

def _kind(name):
    n = name.lower()
    hits = [k for k, kws in KW.items() if any(s in n for s in kws)]
    if "wason" in hits: return "wason"
    if "preset" in hits: return "preset"
    if "line" in hits and (n.endswith(".xlsx") or n.endswith(".xls") or n.endswith(".xlsm")): return "line"
    for k in ("fan","cpu","msu","client","osc","fm","atten"):
        if k in hits: return k
    return hits[0] if hits else None


def find_in_zip(zip_file):
    found = {k: None for k in KW}
    def walk(zf):
        for name in zf.namelist():
            if all(found.values()): return
            if name.endswith("/"): continue
            lname = name.lower()
            if lname.endswith(".zip"):
                try:
                    walk(zipfile.ZipFile(io.BytesIO(zf.read(name))))
                except: pass
                continue
            ext = _ext(lname)
            kind = _kind(lname)
            if not ext or not kind or found[kind]: continue
            try:
                with zf.open(name) as f:
                    df = LOADERS[ext](f)
                found[kind] = (df, name) 
            except: continue
    try:
        walk(zipfile.ZipFile(zip_file))
    except Exception as e:
        st.error(f"Error reading ZIP file: {e}")
        return {}
        
    return found


def safe_copy(obj):
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    return obj

# ====== SIDEBAR (คงเดิม) ======
menu = st.sidebar.radio("เลือกกิจกรรม", [
    "หน้าแรก","Visualization","CPU","FAN","MSU","Line board","Client board",
    "Fiber Flapping","Loss between Core","Loss between EOL","Preset status","APO Remnant","Summary table & report"
])


# ====== หน้าแรก (Calendar Upload + Run Analysis + Delete) ======
if menu == "หน้าแรก":
    st.subheader("DWDM Monitoring Dashboard")
    st.markdown("#### Upload & Manage ZIP Files (Cloud Persistent)")

    chosen_date = st.date_input("Select date", value=date.today())
    files = st.file_uploader(
        "Upload ZIP files",
        type=["zip"],
        accept_multiple_files=True,
        key=f"uploader_{chosen_date}"
    )
    if files:
        if st.button("Upload", key=f"upload_btn_{chosen_date}"):
            if STORAGE_API_URL is None:
                st.error("Cannot upload. Supabase Storage client is not initialized.")
            else:
                for file in files:
                    save_file_to_storage(str(chosen_date), file) # ⬅️ ใช้ save_file_to_storage ใหม่
                st.success(f"Upload completed ({len(files)} file(s))")
                st.rerun()

    st.subheader("Calendar")
    events = []
    for d, cnt in list_dates_with_files():
        events.append({"title": f"{cnt} file(s)", "start": d, "allDay": True, "color": "blue"})

    calendar_res = calendar(
        events=events,
        options={"initialView": "dayGridMonth", "height": "400px", "selectable": True},
        key="calendar",
    )

    if "selected_date" not in st.session_state:
        st.session_state["selected_date"] = str(date.today())

    clicked_date = None
    if calendar_res and calendar_res.get("callback") == "dateClick":
        iso_date = calendar_res["dateClick"]["date"]
        dt_utc = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        dt_th = dt_utc.astimezone(pytz.timezone("Asia/Bangkok"))
        clicked_date = dt_th.date().isoformat()

    if clicked_date:
        st.session_state["selected_date"] = clicked_date

    selected_date = st.session_state["selected_date"]

    st.subheader(f"Files for {selected_date}")
    # ดึง metadata จาก DB
    files_list = list_files_by_date(selected_date)
    
    if not files_list:
        st.info("No files for this date")
    else:
        selected_files_meta = [] # (fid, fname, storage_path)
        for fid, fname, fpath in files_list:
            col1, col2 = st.columns([4, 1])
            with col1:
                checked = st.checkbox(fname, key=f"chk_{fid}")
                if checked:
                    selected_files_meta.append((fid, fname, fpath))
            with col2:
                if st.button("Delete", key=f"del_{fid}"):
                    delete_file(fid) # ⬅️ ลบจาก Storage และ DB
                    st.rerun()

        
        if st.button("Run Analysis", key="analyze_btn"):
            if not selected_files_meta:
                st.warning("Please select at least one file to analyze")
            else:
                clear_all_uploaded_data()
                total = 0
                
                with st.spinner("Downloading files and running analysis..."):
                    for fid, fname, fpath in selected_files_meta:
                        # ⚠️ ดาวน์โหลดไฟล์จาก Storage (fpath = storage_path)
                        zip_bytes = get_file_bytes_from_storage(fpath)
                        if zip_bytes is None:
                            continue # ข้ามถ้าดาวน์โหลดไม่ได้

                        res = find_in_zip(zip_bytes) # ⬅️ ทำ Analysis จาก Bytes ที่โหลดมา

                        for kind, pack in res.items():
                            if not pack: continue
                            df, zname = pack
                            if kind == "wason":
                                st.session_state["wason_log"] = df 
                                st.session_state["wason_file"] = zname
                            else:
                                st.session_state[f"{kind}_data"] = df 
                                st.session_state[f"{kind}_file"] = zname
                        
                        total += 1

                st.session_state["zip_loaded"] = True
                st.success(f"✅ Analysis finished. Processed {total} file(s).")


# ====== เมนูอื่น ๆ (Analyzer Modules - คงเดิม) ======

elif menu == "CPU":
    if st.session_state.get("cpu_data") is not None:
        try:
            df_ref = pd.read_excel("data/CPU.xlsx")
            analyzer = CPU_Analyzer(
                df_cpu=safe_copy(st.session_state.get("cpu_data")),
                df_ref=df_ref.copy(),
                ns="cpu"
            )
            analyzer.process()
            st.session_state["cpu_analyzer"] = analyzer 
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "FAN":
    if st.session_state.get("fan_data") is not None:
        try:
            df_ref = pd.read_excel("data/FAN.xlsx")
            analyzer = FAN_Analyzer(
                df_fan=safe_copy(st.session_state.get("fan_data")),
                df_ref=df_ref.copy(),
                ns="fan"
            )
            analyzer.process()
            st.session_state["fan_analyzer"] = analyzer
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "MSU":
    if st.session_state.get("msu_data") is not None:
        try:
            df_ref = pd.read_excel("data/MSU.xlsx")
            analyzer = MSU_Analyzer(
                df_msu=safe_copy(st.session_state.get("msu_data")),
                df_ref=df_ref.copy(),
                ns="msu"
            )
            analyzer.process()
            st.session_state["msu_analyzer"] = analyzer
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "Line board":
    st.markdown("### Line Cards Performance")
    df_line = st.session_state.get("line_data") 
    log_txt = st.session_state.get("wason_log") 
    if log_txt:
        st.session_state["lb_pmap"] = Line_Analyzer.get_preset_map(log_txt)
    pmap = st.session_state.get("lb_pmap", {})

    if df_line is not None:
        try:
            df_ref = pd.read_excel("data/Line.xlsx")
            analyzer = Line_Analyzer(
                df_line=df_line.copy(), 
                df_ref=df_ref.copy(),
                pmap=pmap,
                ns="line",
            )
            analyzer.process()
            st.caption(
                f"Using LINE file: {st.session_state.get('line_file')}"
                f"{'(with WASON log)' if log_txt else '(no WASON log)'}"
            )
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "Client board":
    st.markdown("### Client Board")
    if st.session_state.get("client_data") is not None:
        try:
            df_ref = pd.read_excel("data/Client.xlsx")
            analyzer = Client_Analyzer(
                df_client=st.session_state.client_data.copy(),
                ref_path="data/Client.xlsx"
            )
            analyzer.process()
            st.session_state["client_analyzer"] = analyzer
            st.caption(f"Using CLIENT file: {st.session_state.get('client_file')}")
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "Fiber Flapping":
    st.markdown("### Fiber Flapping (OSC + FM)")
    df_osc = st.session_state.get("osc_data") 
    df_fm = st.session_state.get("fm_data") 

    if (df_osc is not None) and (df_fm is not None):
        try:
            analyzer = FiberflappingAnalyzer(
                df_optical=df_osc.copy(),
                df_fm=df_fm.copy(),
                threshold=2.0, 
            )
            analyzer.process()
            st.caption(
                f"Using OSC: {st.session_state.get('osc_file')} | "
                f"FM: {st.session_state.get('fm_file')}"
            )
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "Loss between EOL":
    st.markdown("### Loss between EOL")
    df_raw = st.session_state.get("atten_data") 
    if df_raw is not None:
        try:
            analyzer = EOLAnalyzer(
                df_ref=None,
                df_raw_data=df_raw.copy(),
                ref_path="data/EOL.xlsx",
            )
            analyzer.process() 
            st.session_state["eol_analyzer"] = analyzer
            st.caption(f"Using RAW file: {st.session_state.get('atten_file')}")
        except Exception as e:
            st.error(f"An error occurred during EOL analysis: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "Loss between Core":
    st.markdown("### Loss between Core")
    df_raw = st.session_state.get("atten_data")
    if df_raw is not None:
        try:
            analyzer = CoreAnalyzer(
                df_ref=None,
                df_raw_data=df_raw.copy(),
                ref_path="data/EOL.xlsx",
            )
            analyzer.process() 
            st.session_state["core_analyzer"] = analyzer
            st.caption(f"Using RAW file: {st.session_state.get('atten_file')}")
        except Exception as e:
            st.error(f"An error occurred during Core analysis: {e}")
    else:
        st.info("Please run analysis on 'หน้าแรก' to load file data.")


elif menu == "Summary table & report":
    summary = SummaryTableReport()
    summary.render()