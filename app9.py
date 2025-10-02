import os
# import sqlite3 ❌ ลบออก
import uuid
from datetime import datetime, date
import pytz
import streamlit as st
from streamlit_calendar import calendar
import io, zipfile
import pandas as pd

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

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
# DB_FILE = "files.db" ❌ ลบออก


# ====== DB INIT / CONNECTION (ใช้ Streamlit Connection API) ======
try:
    # เปลี่ยนชื่อจาก "supabase" เป็น "supabase_url"
    conn = st.connection("supabase_url", type="sql")
except Exception as e:
    # หากเชื่อมต่อไม่ได้ จะแสดง Error เพื่อให้ผู้ใช้ตรวจสอบ secrets
    st.error(f"Failed to connect to Supabase. Error: {e}")
    conn = None 

# def init_db(): ❌ ลบออก
#     ...
# init_db() ❌ ลบออก


# ====== DB FUNCTIONS (เปลี่ยนไปใช้ Supabase/PostgreSQL) ======
def save_file(upload_date: str, file):
    """บันทึกไฟล์ลงดิสก์ชั่วคราวและบันทึก Metadata ลง Supabase"""
    if conn is None:
        return

    file_id = str(uuid.uuid4())
    stored_name = f"{file_id}_{file.name}"
    stored_path = os.path.join(UPLOAD_DIR, upload_date, stored_name)
    os.makedirs(os.path.dirname(stored_path), exist_ok=True)

    # 1. บันทึกไฟล์ลงดิสก์ชั่วคราว (บน Streamlit Cloud)
    with open(stored_path, "wb") as f:
        f.write(file.getbuffer())

    # 2. บันทึก Metadata ลง Supabase
    current_time_str = datetime.now(pytz.timezone("Asia/Bangkok")).isoformat()

    conn.query(
        """
        INSERT INTO uploads (upload_date, orig_filename, stored_path, created_at)
        VALUES (:upload_date, :orig_filename, :stored_path, :created_at)
        """, 
        params={
            "upload_date": upload_date, 
            "orig_filename": file.name, 
            "stored_path": stored_path, 
            "created_at": current_time_str
        },
        ttl=0 # ไม่แคชการเขียนข้อมูล
    )

@st.cache_data(ttl="1h")
def list_files_by_date(upload_date: str):
    """ดึงรายการไฟล์ทั้งหมดตามวันที่จาก Supabase"""
    if conn is None:
        return []

    # conn.query คืนค่าเป็น DataFrame
    df = conn.query(
        "SELECT id, orig_filename, stored_path FROM uploads WHERE upload_date = :upload_date ORDER BY created_at DESC", 
        params={"upload_date": upload_date}
    )
    # แปลงเป็น list of tuples เพื่อให้เข้ากับโค้ดส่วนแสดงผล
    return list(df[['id', 'orig_filename', 'stored_path']].itertuples(index=False, name=None))

def delete_file(file_id: int):
    """ลบไฟล์ออกจากดิสก์ชั่วคราวและลบ Metadata ออกจาก Supabase"""
    if conn is None:
        return
        
    # 1. ดึง stored_path เพื่อลบไฟล์จากดิสก์ชั่วคราว
    df_path = conn.query(
        "SELECT stored_path FROM uploads WHERE id = :id",
        params={"id": file_id},
        ttl="1h"
    )
    if not df_path.empty:
        try:
            # ลบไฟล์จากดิสก์ (บน Streamlit Cloud ซึ่งไฟล์จะหายไปเองเมื่อรีสตาร์ทอยู่แล้ว)
            os.remove(df_path['stored_path'].iloc[0]) 
        except FileNotFoundError:
            pass 

    # 2. ลบ metadata จาก Supabase
    conn.query(
        "DELETE FROM uploads WHERE id = :id", 
        params={"id": file_id},
        ttl=0
    )

@st.cache_data(ttl="1h")
def list_dates_with_files():
    """ดึงวันที่และจำนวนไฟล์ทั้งหมดจาก Supabase สำหรับ Calendar"""
    if conn is None:
        return []

    # ดึงข้อมูลวันที่และจำนวนไฟล์
    df = conn.query(
        "SELECT upload_date, COUNT(id) as count FROM uploads GROUP BY upload_date",
    )
    # แปลง DataFrame เป็น list of tuples (upload_date, count)
    return list(df.itertuples(index=False, name=None))


# ====== CLEAR SESSION & ZIP PARSER ======
def clear_all_uploaded_data():
    st.session_state.clear()


# ====== ZIP PARSER ======
KW = {
    "cpu": ("cpu",),
    "fan": ("fan",),
    "msu": ("msu",),
    "client": ("client", "client board"),
    "line": ("line","line board"),
    "wason": ("wason","log"), 
    "osc": ("osc","osc optical"),
    "fm": ("fm","alarm","fault management"),
    "atten": ("optical attenuation report", "optical_attenuation_report"),
    "atten": ("optical attenuation report","optical attenuation"),
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

    # ---- Priority ----
    if "wason" in hits:
        return "wason"
    if "preset" in hits:
        return "preset"

    # ---- เช็คว่า line ต้องเป็น Excel เท่านั้น ----
    if "line" in hits and (n.endswith(".xlsx") or n.endswith(".xls") or n.endswith(".xlsm")):
        return "line"

    # ---- อื่น ๆ ตามปกติ ----
    for k in ("fan","cpu","msu","client","osc","fm","atten"):
        if k in hits:
            return k

    return hits[0] if hits else None


def find_in_zip(zip_file):
    found = {k: None for k in KW}
    def walk(zf):
        for name in zf.namelist():
            if all(found.values()): 
                return
            if name.endswith("/"): 
                continue
            lname = name.lower()
            if lname.endswith(".zip"):
                try:
                    walk(zipfile.ZipFile(io.BytesIO(zf.read(name))))
                except:
                    pass
                continue
            ext = _ext(lname)
            kind = _kind(lname)
            if not ext or not kind or found[kind]:
                continue
            try:
                with zf.open(name) as f:
                    df = LOADERS[ext](f)
                    print("DEBUG LOADED:", kind, type(df), name)

                # ถ้าเป็น log (.txt) → เก็บเป็น string ใน key "wason_log"
                if kind == "wason":
                    found[kind] = (df, name) # df = string
                else:
                    found[kind] = (df, name) # df = DataFrame

            except:
                continue
    walk(zipfile.ZipFile(zip_file))
    return found


def safe_copy(obj):
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    return obj

# ====== SIDEBAR ======
menu = st.sidebar.radio("เลือกกิจกรรม", [
    "หน้าแรก","Visualization","CPU","FAN","MSU","Line board","Client board",
    "Fiber Flapping","Loss between Core","Loss between EOL","Preset status","APO Remnant","Summary table & report"
])


# ====== หน้าแรก (Calendar Upload + Run Analysis + Delete) ======
if menu == "หน้าแรก":
    st.subheader("DWDM Monitoring Dashboard")
    st.markdown("#### Upload & Manage ZIP Files (with Calendar)")

    chosen_date = st.date_input("Select date", value=date.today())
    files = st.file_uploader(
        "Upload ZIP files",
        type=["zip"],
        accept_multiple_files=True,
        key=f"uploader_{chosen_date}"
    )
    if files:
        if st.button("Upload", key=f"upload_btn_{chosen_date}"):
            for file in files:
                save_file(str(chosen_date), file)
            st.success("Upload completed")
            st.rerun()

    st.subheader("Calendar")
    events = []
    # ใช้ list_dates_with_files ที่เชื่อมต่อ Supabase แล้ว
    for d, cnt in list_dates_with_files():
        events.append({
            "title": f"{cnt} file(s)",
            "start": d,
            "allDay": True,
            "color": "blue"
        })

    calendar_res = calendar(
        events=events,
        options={
            "initialView": "dayGridMonth",
            "height": "400px",
            "selectable": True,
        },
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
    # ใช้ list_files_by_date ที่เชื่อมต่อ Supabase แล้ว
    files_list = list_files_by_date(selected_date)
    if not files_list:
        st.info("No files for this date")
    else:
        selected_files = []
        for fid, fname, fpath in files_list:
            col1, col2 = st.columns([4, 1])
            with col1:
                checked = st.checkbox(fname, key=f"chk_{fid}")
                if checked:
                    selected_files.append((fid, fname, fpath))
            with col2:
                # ใช้ delete_file ที่เชื่อมต่อ Supabase แล้ว
                if st.button("Delete", key=f"del_{fid}"):
                    delete_file(fid)
                    st.rerun()

        
        if st.button("Run Analysis", key="analyze_btn"):
            if not selected_files:
                st.warning("Please select at least one file to analyze")
            else:
                clear_all_uploaded_data()
                total = 0
                for fid, fname, fpath in selected_files:
                    with open(fpath, "rb") as f:
                        zip_bytes = io.BytesIO(f.read())
                        res = find_in_zip(zip_bytes)
                    for kind, pack in res.items():
                        if not pack:
                            continue
                        df, zname = pack
                        if kind == "wason":
                            st.session_state["wason_log"] = df # ✅ string log
                            st.session_state["wason_file"] = zname
                        else:
                            st.session_state[f"{kind}_data"] = df # ✅ DataFrame
                            st.session_state[f"{kind}_file"] = zname

                    total += 1

                st.session_state["zip_loaded"] = True


                # 🔹 แก้ตรงนี้ให้แสดงว่าเสร็จแล้ว
                st.success("✅ Analysis finished")


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
        st.info("Please upload file to start the analysis")


elif menu == "FAN":
    if st.session_state.get("fan_data") is not None:
        try:
            df_ref = pd.read_excel("data/FAN.xlsx")
            analyzer = FAN_Analyzer(
                df_fan=safe_copy(st.session_state.get("fan_data")),
                df_ref=df_ref.copy(),
                ns="fan" # namespace สำหรับ cascading_filter
            )
            analyzer.process()
            st.session_state["fan_analyzer"] = analyzer
            st.write("DEBUG set fan_analyzer", st.session_state["fan_analyzer"])

        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please upload a FAN file to start the analysis")


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
        st.info("Please upload an MSU file to start the analysis")


elif menu == "Line board":
    st.markdown("### Line Cards Performance")

    df_line = st.session_state.get("line_data") # ✅ DataFrame
    log_txt = st.session_state.get("wason_log") # ✅ String

    # gen pmap จาก TXT ถ้ามี
    if log_txt:
        st.session_state["lb_pmap"] = Line_Analyzer.get_preset_map(log_txt)
    pmap = st.session_state.get("lb_pmap", {})

    if df_line is not None:
        try:
            df_ref = pd.read_excel("data/Line.xlsx")
            analyzer = Line_Analyzer(
                df_line=df_line.copy(), # ✅ ต้องเป็น DataFrame
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
        st.info("Please upload a ZIP on 'หน้าแรก' that contains a Line workbook")



elif menu == "Client board":
    st.markdown("### Client Board")
    if st.session_state.get("client_data") is not None:
        try:
            # โหลด Reference
            df_ref = pd.read_excel("data/Client.xlsx")
            
            # สร้าง Analyzer
            analyzer = Client_Analyzer(
                df_client=st.session_state.client_data.copy(),
                ref_path="data/Client.xlsx" # ✅ ให้ class โหลดเอง
            )
            analyzer.process()
            st.session_state["client_analyzer"] = analyzer
            st.caption(f"Using CLIENT file: {st.session_state.get('client_file')}")
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please upload a ZIP on 'หน้าแรก' that contains a Client workbook")


elif menu == "Fiber Flapping":
    st.markdown("### Fiber Flapping (OSC + FM)")

    df_osc = st.session_state.get("osc_data") # จาก ZIP: .xlsx → DataFrame
    df_fm = st.session_state.get("fm_data") # จาก ZIP: .xlsx → DataFrame

    if (df_osc is not None) and (df_fm is not None):
        try:
            analyzer = FiberflappingAnalyzer(
                df_optical=df_osc.copy(),
                df_fm=df_fm.copy(),
                threshold=2.0, # คงเดิม
            )
            analyzer.process()
            st.caption(
                f"Using OSC: {st.session_state.get('osc_file')} | "
                f"FM: {st.session_state.get('fm_file')}"
            )
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.info("Please upload a ZIP on 'หน้าแรก' that contains both OSC (optical) and FM workbooks.")



elif menu == "Loss between EOL":
    st.markdown("### Loss between EOL")
    df_raw = st.session_state.get("atten_data") # ใช้ atten_data ที่โหลดมา
    if df_raw is not None:
        try:
            analyzer = EOLAnalyzer(
                df_ref=None,
                df_raw_data=df_raw.copy(),
                ref_path="data/EOL.xlsx",
            )
            analyzer.process() # ⬅ ตรงนี้ทำให้โชว์ทันที
            st.session_state["eol_analyzer"] = analyzer
            st.caption(f"Using RAW file: {st.session_state.get('atten_file')}")
        except Exception as e:
            st.error(f"An error occurred during EOL analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains the attenuation report.")


elif menu == "Loss between Core":
    st.markdown("### Loss between Core")
    df_raw = st.session_state.get("atten_data") # ใช้ atten_data เหมือนกัน
    if df_raw is not None:
        try:
            analyzer = CoreAnalyzer(
                df_ref=None,
                df_raw_data=df_raw.copy(),
                ref_path="data/EOL.xlsx",
            )
            analyzer.process() # ⬅ ตรงนี้ทำให้โชว์ทันที
            st.session_state["core_analyzer"] = analyzer
            st.caption(f"Using RAW file: {st.session_state.get('atten_file')}")
        except Exception as e:
            st.error(f"An error occurred during Core analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains the attenuation report.")



elif menu == "Summary table & report":
    summary = SummaryTableReport()
    summary.render()