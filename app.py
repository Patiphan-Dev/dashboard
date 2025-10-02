import os
import uuid
from datetime import datetime, date
import pytz
import streamlit as st
from streamlit_calendar import calendar
import io, zipfile
import pandas as pd
import postgrest # สำหรับจัดการ Error ของ Supabase Storage

# 💡 NEW: Import Supabase Client
from supabase import create_client, Client


# ====== IMPORT ANALYZERS (etc.) ======
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


# ====== DB INIT (SUPABASE) ======
# 💡 แก้ไข: ฟังก์ชันนี้จะคืนค่า Client และชื่อ Table/Bucket เป็นตัวแปร Global
@st.cache_resource
def init_supabase() -> (Client, str, str):
    # ดึงค่าจาก .streamlit/secrets.toml
    url = st.secrets.SUPABASE_URL
    key = st.secrets.SUPABASE_KEY
    
    # กำหนดค่าคงที่ Global
    SB_TABLE = "uploads" # ชื่อ Table ใน Supabase DB
    SB_BUCKET = st.secrets.SUPABASE_BUCKET # ชื่อ Bucket ใน Supabase Storage
    
    # สร้าง Client และเชื่อมต่อ
    supabase: Client = create_client(url, key)
    
    # ส่งคืน Client และชื่อ Table/Bucket
    return supabase, SB_TABLE, SB_BUCKET

# 💡 NEW: รับค่าที่ส่งคืนกลับมาเป็นตัวแปร Global
try:
    supabase, SB_TABLE, SB_BUCKET = init_supabase()
    # 💡 NEW DEBUG: ตรวจสอบการเชื่อมต่อกับตาราง
    st.sidebar.success(f"Supabase Client Initialized: {SB_TABLE}") 
    
except Exception as e:
    st.sidebar.error("🚨 Error Initializing Supabase Connection. Check your .streamlit/secrets.toml file.")
    st.stop()

# ====== DB FUNCTIONS (SUPABASE) ======
def save_file(upload_date: str, file):
    """บันทึกไฟล์ไปที่ Supabase Storage และ Metadata ไปที่ Supabase DB"""
    # 💡 ใช้ตัวแปร Global
    table_name = SB_TABLE
    bucket_name = SB_BUCKET
    file_id = str(uuid.uuid4())
    orig_filename = file.name
    
    # --- 1. Upload file to Supabase Storage ---
    stored_path_in_bucket = f"{upload_date}/{file_id}_{orig_filename}"
    
    try:
        supabase.storage.from_(bucket_name).upload(
            file=file.getbuffer(), 
            path=stored_path_in_bucket, 
            file_options={"content-type": "application/zip"}
        )
    except Exception as e:
        if "The resource already exists" not in str(e):
             st.error(f"Error uploading file to storage: {e}")
        return

    # --- 2. Save metadata to PostgreSQL Table ---
    data_to_insert = {
        "upload_date": upload_date,
        "orig_filename": orig_filename,
        "stored_path": stored_path_in_bucket, 
        "created_at": datetime.now().isoformat(),
    }
    
    try:
        supabase.table(table_name).insert(data_to_insert).execute()
    except Exception as e:
        st.error(f"Failed to save metadata to database: {e}")


def list_files_by_date(upload_date: str):
    """ดึงรายการไฟล์จาก Supabase DB ตามวันที่"""
    table_name = SB_TABLE
    try:
        response = (
            supabase.table(table_name)
            .select("id, orig_filename, stored_path")
            .eq("upload_date", upload_date)
            .order("created_at", desc=True) # เรียงจากใหม่ไปเก่า
            .execute()
        )
        rows = [(d['id'], d['orig_filename'], d['stored_path']) for d in response.data]
        return rows
    except Exception as e:
        st.error(f"Error listing files: {e}")
        return []

def delete_file(file_id: int):
    """ลบไฟล์จาก Storage และ Metadata จาก DB"""
    # 💡 ใช้ตัวแปร Global
    table_name = SB_TABLE
    bucket_name = SB_BUCKET

    # --- 1. Find stored_path first ---
    try:
        response = supabase.table(table_name).select("stored_path").eq("id", file_id).limit(1).execute()
        row = response.data[0] if response.data else None
    except Exception as e:
        st.error(f"Error fetching path for deletion: {e}")
        return

    if row:
        stored_path_in_bucket = row["stored_path"]
        
        # --- 2. Delete file from Supabase Storage ---
        try:
            supabase.storage.from_(bucket_name).remove([stored_path_in_bucket])
        except postgrest.exceptions.APIError as e:
            st.warning(f"File not found or error removing from storage: {e}")
        except Exception as e:
            st.warning(f"Unexpected error removing from storage: {e}")

        # --- 3. Delete metadata from PostgreSQL Table ---
        try:
            supabase.table(table_name).delete().eq("id", file_id).execute()
        except Exception as e:
            st.error(f"Error deleting metadata: {e}")
            
def list_dates_with_files():
    """ดึงวันที่ที่มีไฟล์และจำนวนไฟล์สำหรับ Calendar"""
    table_name = SB_TABLE
    try:
        response = supabase.table(table_name).select("upload_date").execute()
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return []

        date_counts = df.groupby("upload_date").size().reset_index(name='count')
        
        rows = list(date_counts.itertuples(index=False, name=None))
        return rows
    except Exception as e:
        st.error(f"Error fetching dates for calendar: {e}")
        return []

# ====== CLEAR SESSION ======
def clear_all_uploaded_data():
    st.session_state.clear()


# ====== ZIP PARSER ======
KW = {
    "cpu": ("cpu",),
    "fan": ("fan",),
    "msu": ("msu",),
    "client": ("client", "client board"),
    "line":  ("line","line board"),       
    "wason": ("wason","log"), 
    "osc": ("osc","osc optical"),       
    "fm":  ("fm","alarm","fault management"),
    "atten": ("optical attenuation report", "optical_attenuation_report"),
    "atten": ("optical attenuation report","optical attenuation"),
    "preset": ("mobaxterm", "moba xterm", "moba"),
}

LOADERS = {
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel,
    ".txt":  lambda f: f.read().decode("utf-8", errors="ignore"),
}

def _ext(name: str) -> str:
    name = name.lower()
    return next((e for e in LOADERS if name.endswith(e)), "")

def _kind(name):
    n = name.lower()
    hits = [k for k, kws in KW.items() if any(s in n for s in kws)]

    if "wason" in hits:
        return "wason"
    if "preset" in hits:
        return "preset"

    if "line" in hits and (n.endswith(".xlsx") or n.endswith(".xls") or n.endswith(".xlsm")):
        return "line"

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

                if kind == "wason":
                    found[kind] = (df, name)
                else:
                    found[kind] = (df, name)

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
    # 💡 แก้ไข: ใช้ตัวแปร Global SB_TABLE และ SB_BUCKET
    st.caption(f"Database: Supabase ({SB_TABLE}) | Storage: Supabase Storage ({SB_BUCKET})") 

    chosen_date = st.date_input("Select date", value=date.today())
    files = st.file_uploader(
        "Upload ZIP files",
        type=["zip"],
        accept_multiple_files=True,
        key=f"uploader_{chosen_date}"
    )
    if files:
        if st.button("Upload to Supabase", key=f"upload_btn_{chosen_date}"):
            for file in files:
                save_file(str(chosen_date), file)
            st.success("Upload completed to Supabase Storage and Database")
            st.rerun()

    st.subheader("Calendar")
    events = []
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
                if st.button("Delete", key=f"del_{fid}"):
                    delete_file(fid)
                    st.rerun()

        
        if st.button("Run Analysis", key="analyze_btn"):
            if not selected_files:
                st.warning("Please select at least one file to analyze")
            else:
                clear_all_uploaded_data()
                total = 0
                
                # 💡 NEW: Logic การโหลดไฟล์จาก Supabase Storage
                for fid, fname, fpath in selected_files:
                    try:
                        with st.spinner(f"Downloading {fname} from Supabase..."):
                            # 💡 ใช้ download() เพื่อดึงไฟล์ ZIP เป็น bytes
                            zip_bytes_data = (
                                supabase.storage
                                # 💡 ใช้ตัวแปร Global SB_BUCKET
                                .from_(SB_BUCKET)
                                .download(fpath) 
                            )
                        
                        zip_bytes = io.BytesIO(zip_bytes_data) 
                        
                        res = find_in_zip(zip_bytes)
                        
                        # โค้ดส่วนการเก็บผลลัพธ์ลง st.session_state เหมือนเดิม
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

                    except Exception as e:
                        st.error(f"Error processing file {fname}: {e}")
                        continue

                st.session_state["zip_loaded"] = True
                st.success("✅ Analysis finished and data loaded into memory.")


# ====== Analysis Pages (ใช้โค้ดเดิม) ======

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
                ns="fan"
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
                f"Using LINE file: {st.session_state.get('line_file')}  "
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
        st.info("Please upload a ZIP on 'หน้าแรก' that contains a Client workbook")


elif menu == "Fiber Flapping":
    st.markdown("### Fiber Flapping (OSC + FM)")

    df_osc = st.session_state.get("osc_data")
    df_fm  = st.session_state.get("fm_data")

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
        st.info("Please upload a ZIP on 'หน้าแรก' that contains both OSC (optical) and FM workbooks.")



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
        st.info("Please upload a ZIP file that contains the attenuation report.")


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
        st.info("Please upload a ZIP file that contains the attenuation report.")


elif menu == "Summary table & report":
    summary = SummaryTableReport()
    summary.render()