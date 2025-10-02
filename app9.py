import os
import uuid
from datetime import datetime, date
import pytz
import streamlit as st
from streamlit_calendar import calendar
import io, zipfile
import pandas as pd
from sqlalchemy import text # NEW: ใช้สำหรับ INSERT/DELETE

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

# ⚠️ UPLOAD_DIR ยังคงมี แต่เราจะไม่ใช้ Disk I/O อีก
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ====== DB INIT / CONNECTION (ใช้ Streamlit Connection API) ======
try:
    # ใช้ชื่อมาตรฐาน "supabase" (ต้องตรงกับ Secrets ที่ตั้งไว้)
    conn = st.connection("supabase", type="sql")
except Exception as e:
    # หากเชื่อมต่อไม่ได้ จะแสดง Error เพื่อให้ผู้ใช้ตรวจสอบ secrets
    st.error(f"Failed to connect to Supabase. Error: {e}")
    conn = None 


# ====== DB FUNCTIONS (Cloud-Safe: บันทึกเฉพาะ Metadata) ======
def save_file_metadata_only(upload_date: str, file_name: str):
    """บันทึก Metadata ลง Supabase (ไม่บันทึกไฟล์จริงลงดิสก์)"""
    if conn is None:
        return

    current_time_str = datetime.now(pytz.timezone("Asia/Bangkok")).isoformat()

    # ใช้ session.execute สำหรับ INSERT (แก้ ResourceClosedError)
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
                "orig_filename": file_name, 
                # เก็บ path เป็น dummy เพื่อให้ schema เดิมใช้งานได้
                "stored_path": f"CLOUD_ONLY_{str(uuid.uuid4())}", 
                "created_at": current_time_str
            }
        )
        session.commit()
        
# ⚠️ ฟังก์ชัน list_files_by_date และ delete_file ถูกลบออก
# ⚠️ เนื่องจากไฟล์จริงที่อ้างถึงด้วย stored_path จะไม่สามารถดึงกลับมาได้แล้ว
# ⚠️ และเราเปลี่ยนไปใช้การวิเคราะห์ไฟล์ที่เพิ่งอัปโหลดทันที

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


# ====== CLEAR SESSION & ZIP PARSER (คงเดิม) ======
def clear_all_uploaded_data():
    st.session_state.clear()


# ====== ZIP PARSER (คงเดิม) ======
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
                    # ใช้ LOADERS[ext](f) จาก Bytes โดยตรง
                    df = LOADERS[ext](f) 
                    print("DEBUG LOADED:", kind, type(df), name)

                if kind == "wason":
                    found[kind] = (df, name) 
                else:
                    found[kind] = (df, name) 

            except:
                continue
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


# ====== หน้าแรก (Calendar Upload + Run Analysis ทันที) ======
if menu == "หน้าแรก":
    st.subheader("DWDM Monitoring Dashboard")
    st.markdown("#### Upload & Run Analysis (Cloud-Safe)")

    chosen_date = st.date_input("Select date", value=date.today())
    files = st.file_uploader(
        "Upload ZIP files",
        type=["zip"],
        accept_multiple_files=True,
        key=f"uploader_{chosen_date}"
    )

    # 1. รัน Analysis ทันทีที่กดปุ่ม
    if st.button("Run Analysis", key="analyze_btn_upload_page"):
        if not files:
            st.warning("Please upload at least one ZIP file to analyze.")
        else:
            clear_all_uploaded_data()
            total = 0
            
            # เก็บข้อมูลไฟล์ทั้งหมดใน session state เพื่อใช้ในหน้าอื่น
            st.session_state["uploaded_file_info"] = [] 

            with st.spinner(f"Analyzing {len(files)} files..."):
                for file in files:
                    # 1. บันทึก Metadata (สำหรับ Calendar)
                    save_file_metadata_only(str(chosen_date), file.name)
                    
                    # 2. ทำ Analysis จาก Bytes โดยตรง (แก้ FileNotFoundError)
                    zip_bytes = io.BytesIO(file.getbuffer())
                    res = find_in_zip(zip_bytes)
                    
                    for kind, pack in res.items():
                        if not pack:
                            continue
                        df, zname = pack
                        if kind == "wason":
                            st.session_state["wason_log"] = df 
                            st.session_state["wason_file"] = zname
                        else:
                            st.session_state[f"{kind}_data"] = df 
                            st.session_state[f"{kind}_file"] = zname
                    
                    total += 1
                    st.session_state["uploaded_file_info"].append(file.name)

            st.session_state["zip_loaded"] = True
            st.success(f"✅ Analysis finished. Processed {total} file(s).")
            st.rerun() # reruns เพื่อให้ Calendar อัปเดต

    st.subheader("Calendar (Date History)")
    
    events = []
    # ใช้ list_dates_with_files ที่เชื่อมต่อ Supabase แล้ว
    for d, cnt in list_dates_with_files():
        events.append({
            "title": f"{cnt} upload(s)",
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
    
    # แสดงรายการไฟล์ที่เพิ่งวิเคราะห์ไป
    if st.session_state.get("uploaded_file_info"):
        st.subheader("Last Analyzed Files:")
        for name in st.session_state["uploaded_file_info"]:
            st.markdown(f"- **{name}**")


# ====== เมนูอื่น ๆ (คงเดิม) ======

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
            # แสดงผล
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
            # แสดงผล
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
            # แสดงผล
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
            # แสดงผล
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
            # แสดงผล
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
            # แสดงผล
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
            # แสดงผล
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
            # แสดงผล
        except Exception as e:
            st.error(f"An error occurred during Core analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains the attenuation report.")


elif menu == "Summary table & report":
    summary = SummaryTableReport()
    summary.render()