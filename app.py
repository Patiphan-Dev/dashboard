import os
import uuid
from datetime import datetime, date
import pytz
import streamlit as st
from streamlit_calendar import calendar
import io, zipfile
import pandas as pd
from st_supabase_connection import SupabaseConnection
from supabase import create_client # สำหรับจัดการ Storage และ Client ดิบ

# ====== IMPORT ANALYZERS ======
from CPU_Analyzer import CPU_Analyzer
from FAN_Analyzer import FAN_Analyzer
from MSU_Analyzer import MSU_Analyzer
from Line_Analyzer import Line_Analyzer
from Client_Analyzer import Client_Analyzer
from Fiberflapping_Analyzer import FiberflappingAnalyzer
from EOL_Core_Analyzer import EOLAnalyzer, CoreAnalyzer
from Preset_Analyzer import PresetStatusAnalyzer, render_preset_ui # เพิ่มตามโค้ดเดิม
from APO_Analyzer import ApoRemnantAnalyzer, apo_kpi # เพิ่มตามโค้ดเดิม
from viz import render_visualization # เพิ่มตามโค้ดเดิม
from table1 import SummaryTableReport


# ====== CONFIG ======
st.set_page_config(layout="wide")
pd.set_option("styler.render.max_elements", 1_200_000)


# ==========================================================
# ====== SUPABASE INIT & FUNCTIONS (แทนที่ SQLite ทั้งหมด) ======
# ==========================================================

# ดึง Keys จาก .streamlit/secrets.toml
# ต้องแน่ใจว่าไฟล์ .streamlit/secrets.toml มีค่าเหล่านี้: SUPABASE_URL, SUPABASE_KEY, SUPABASE_BUCKET
try:
    SUPABASE_BUCKET = st.secrets["SUPABASE_BUCKET"]
except KeyError:
    st.error("Error: Please set SUPABASE_BUCKET in .streamlit/secrets.toml")
    st.stop()


@st.cache_resource
def get_supabase_client_db():
    # สำหรับ Query ตาราง (PostgreSQL) - ใช้ st-supabase-connection
    return st.connection("supabase", type=SupabaseConnection)

@st.cache_resource
def get_supabase_client_raw():
    # สำหรับจัดการ Storage (Upload/Download) - ใช้ supabase-py
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

# กำหนดตัวแปรสำหรับเรียกใช้งาน
supabase_db = get_supabase_client_db()
supabase_raw = get_supabase_client_raw()


# ====== SUPABASE FUNCTIONS ======

def save_file(upload_date: str, file):
    import io
    import uuid

    # Generate unique filename
    file_id = str(uuid.uuid4())
    stored_name = f"{file_id}_{file.name}"

    # อ่านไฟล์เป็น bytes
    file_bytes = file.read()  # ✅ ใช้ bytes ไม่ใช่ memoryview

    # Upload ไป Supabase Storage
    try:
        supabase_raw.storage.from_(SUPABASE_BUCKET).upload(
            stored_name,
            file_bytes
        )
        st.success(f"Uploaded {file.name} to bucket {SUPABASE_BUCKET}")
    except Exception as e:
        st.error(f"Failed to upload {file.name}: {e}")
        return

    # Insert metadata ลง PostgreSQL
    supabase_db.table("uploads").insert({
        "upload_date": upload_date,
        "orig_filename": file.name,
        "stored_path": stored_name,
        "created_at": datetime.now().isoformat()
    }).execute()

    st.write(f"✅ Metadata saved for {file.name}")


def debug_file_status(file_id, stored_name):
    # ตรวจสอบใน table
    res = supabase_db.table("uploads").select("*").eq("id", file_id).execute()
    in_table = bool(res.data)

    # ตรวจสอบใน Storage
    files_in_bucket = supabase_raw.storage.from_(SUPABASE_BUCKET).list()
    in_storage = any(f['name'] == stored_name for f in files_in_bucket)

    st.write(f"File {file_id}: in_table={in_table}, in_storage={in_storage}")
    return in_table and in_storage

def list_files_by_date(upload_date: str):
    res = supabase_db.table("uploads").select("id, orig_filename, stored_path").eq("upload_date", upload_date).execute()
    if not res.data:
        st.info(f"No files found for {upload_date}")
        return []
    return [(r['id'], r['orig_filename'], r['stored_path']) for r in res.data]
y

def delete_file(file_id: str): 
    """1. Remove file from Supabase Storage. 2. Delete Metadata from PostgreSQL."""
    
    # 1. ดึง stored_path ก่อนลบ
    # id ถูกเปลี่ยนเป็น string ในตาราง Supabase
    res = supabase_db.table("uploads").select("stored_path").eq("id", file_id).limit(1).execute()
    
    if res.data:
        stored_path = res.data[0]['stored_path']
        
        # 2. ลบไฟล์ออกจาก Supabase Storage
        try:
            # ต้องส่งเป็น list ของ path ที่จะลบ
            supabase_raw.storage.from_(SUPABASE_BUCKET).remove([stored_path])
        except:
            pass # ถ้าลบจาก Storage ไม่ได้ ก็ยังลบ metadata ต่อไปได้
            
    # 3. ลบ Metadata ออกจาก PostgreSQL
    supabase_db.table("uploads").delete().eq("id", file_id).execute()
    st.rerun()


# app9_supabase.py (แทนที่ฟังก์ชัน list_dates_with_files() ทั้งหมด)
def list_dates_with_files():
    """
    ดึงวันที่ และจำนวนไฟล์ที่อัปโหลดในแต่ละวัน
    """
    res = supabase_db.table("uploads") \
        .select("upload_date") \
        .order("upload_date", desc=True) \
        .execute()

    if not res.data:
        return []

    # รวมกลุ่ม (group by) แล้วนับเองด้วย pandas
    df = pd.DataFrame(res.data)
    grouped = df.groupby("upload_date").size().reset_index(name="count")
    rows = [(row["upload_date"], int(row["count"])) for _, row in grouped.iterrows()]
    return rows

# ==========================================================
# ====== จบส่วน SUPABASE FUNCTIONS ======
# ==========================================================


# ====== CLEAR SESSION ======
def clear_all_uploaded_data():
    st.session_state.clear()


# ====== ZIP PARSER (ใช้โค้ดเดิม) ======
KW = {
    "cpu": ("cpu",),
    "fan": ("fan",),
    "msu": ("msu",),
    "client": ("client", "client board"),
    "line": 	("line","line board"), 		
    "wason": ("wason","log"), 
    "osc": ("osc","osc optical"), 		
    "fm": 	("fm","alarm","fault management"),
    "atten": ("optical attenuation report", "optical_attenuation_report"),
    "atten": ("optical attenuation report","optical attenuation"),
    "preset": ("mobaxterm", "moba xterm", "moba"),
    "apo": ("aplus", "aplus log"), # เพิ่มสำหรับ APO (ถ้ามี)
}

LOADERS = {
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel,
    ".txt": 	lambda f: f.read().decode("utf-8", errors="ignore"),
}

def _ext(name: str) -> str:
    name = name.lower()
    return next((e for e in LOADERS if name.endswith(e)), "")

def _kind(name):
    n = name.lower()
    hits = [k for k, kws in KW.items() if any(s in n for s in kws)]

    # ---- Priority ----
    if "wason" in hits and n.endswith(".txt"):
        return "wason"
    if "preset" in hits and n.endswith(".txt"):
        return "preset"
    # ถ้าเป็น Aplus Log
    if "apo" in hits and (n.endswith(".xlsx") or n.endswith(".xls")):
        return "apo"

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
            # if all(found.values()): # ไม่ควร break, ควรประมวลผลให้หมด
            # 	return
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
                    # print("DEBUG LOADED:", kind, type(df), name)

                # ถ้าเป็น log (.txt) → เก็บเป็น string
                if kind in ("wason", "preset"):
                    found[kind] = (df, name)   # df = string
                else:
                    found[kind] = (df, name)   # df = DataFrame

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
    st.markdown("#### Upload & Manage ZIP Files (Cloud Storage)")

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
                # 🚀 เรียกใช้ Supabase save_file()
                save_file(str(chosen_date), file)
            st.rerun()

    st.subheader("Calendar")
    events = []
    # 🚀 เรียกใช้ Supabase list_dates_with_files()
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
        # แปลง UTC เป็น Bangkok Time
        dt_utc = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        dt_th = dt_utc.astimezone(pytz.timezone("Asia/Bangkok"))
        clicked_date = dt_th.date().isoformat()

    if clicked_date:
        st.session_state["selected_date"] = clicked_date

    selected_date = st.session_state["selected_date"]

    st.subheader(f"Files for {selected_date}")
    # 🚀 เรียกใช้ Supabase list_files_by_date()
    files_list = list_files_by_date(selected_date)
    
    if not files_list:
        st.info("No files for this date")
    else:
        selected_files = []
        for fid, fname, fpath in files_list:
            col1, col2 = st.columns([4, 1])
            with col1:
                # fid เป็น UUID (string) ใน Supabase
                checked = st.checkbox(fname, key=f"chk_{fid}")
                if checked:
                    # fpath คือ stored_path ใน Supabase Storage
                    selected_files.append((fid, fname, fpath)) 
            with col2:
                # 🚀 เรียกใช้ Supabase delete_file()
                if st.button("Delete", key=f"del_{fid}"):
                    delete_file(fid) 
                    # delete_file มี st.rerun() อยู่แล้ว

        
        if st.button("Run Analysis", key="analyze_btn"):
            if not selected_files:
                st.warning("Please select at least one file to analyze")
            else:
                clear_all_uploaded_data()
                total = 0
                
                # ============ แก้ไขส่วน RUN ANALYSIS (Download from Supabase) ============
                for fid, fname, fpath in selected_files:
                    try:
                        with st.spinner(f"Downloading and processing {fname} from Cloud..."):
                             # 🚀 ดาวน์โหลดไฟล์ ZIP จาก Supabase Storage โดยใช้ fpath (stored_path)
                            file_bytes = supabase_raw.storage.from_(SUPABASE_BUCKET).download(fpath)
                            
                            zip_bytes = io.BytesIO(file_bytes)
                            res = find_in_zip(zip_bytes)
                            
                        st.success(f"Processed {fname}")

                    except Exception as e:
                        st.error(f"Error downloading or processing {fname} from Supabase: {e}")
                        continue # ข้ามไฟล์ที่มีปัญหา
                        
                    # บันทึกผลลัพธ์ลง Session State (ใช้โค้ดเดิม)
                    for kind, pack in res.items():
                        if not pack:
                            continue
                        df, zname = pack
                        
                        # แยกประเภทข้อมูล (Log/Excel) และบันทึกลง session state
                        if kind in ("wason", "preset"):
                            st.session_state[f"{kind}_log"] = df 	# ✅ string log
                            st.session_state[f"{kind}_file"] = zname
                        else:
                            st.session_state[f"{kind}_data"] = df # ✅ DataFrame
                            st.session_state[f"{kind}_file"] = zname

                    total += 1
                # ============ จบส่วน RUN ANALYSIS ============

                st.session_state["zip_loaded"] = True
                st.success(f"✅ Analysis finished for {total} selected file(s)")


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
                ns="fan"  # namespace สำหรับ cascading_filter
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

    df_line = st.session_state.get("line_data") 	# ✅ DataFrame
    log_txt = st.session_state.get("wason_log") 	# ✅ String

    # gen pmap จาก TXT ถ้ามี
    if log_txt:
        st.session_state["lb_pmap"] = Line_Analyzer.get_preset_map(log_txt)
    pmap = st.session_state.get("lb_pmap", {})

    if df_line is not None:
        try:
            df_ref = pd.read_excel("data/Line.xlsx")
            analyzer = Line_Analyzer(
                df_line=df_line.copy(), 	# ✅ ต้องเป็น DataFrame
                df_ref=df_ref.copy(),
                pmap=pmap,
                ns="line",
            )
            analyzer.process()
            st.session_state["line_analyzer"] = analyzer # ⬅ เก็บ analyzer
            st.caption(
                f"Using LINE file: {st.session_state.get('line_file')} 	"
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
            # df_ref = pd.read_excel("data/Client.xlsx") # ให้ class โหลดเอง
            
            # สร้าง Analyzer
            analyzer = Client_Analyzer(
                df_client=st.session_state.client_data.copy(),
                ref_path="data/Client.xlsx" 	# ✅ ให้ class โหลดเอง
            )
            analyzer.process()
            st.session_state["client_analyzer"] = analyzer # ⬅ เก็บ analyzer
            st.caption(f"Using CLIENT file: {st.session_state.get('client_file')}")
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
    else:
        st.info("Please upload a ZIP on 'หน้าแรก' that contains a Client workbook")


elif menu == "Fiber Flapping":
    st.markdown("### Fiber Flapping (OSC + FM)")

    df_osc = st.session_state.get("osc_data") 	# จาก ZIP: .xlsx → DataFrame
    df_fm 	= st.session_state.get("fm_data") 	# จาก ZIP: .xlsx → DataFrame

    if (df_osc is not None) and (df_fm is not None):
        try:
            analyzer = FiberflappingAnalyzer(
                df_optical=df_osc.copy(),
                df_fm=df_fm.copy(),
                threshold=2.0, 	# คงเดิม
            )
            analyzer.process()
            st.session_state["ff_analyzer"] = analyzer # ⬅ เก็บ analyzer
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
    df_raw = st.session_state.get("atten_data") 	# ใช้ atten_data ที่โหลดมา
    if df_raw is not None:
        try:
            analyzer = EOLAnalyzer(
                df_ref=None,
                df_raw_data=df_raw.copy(),
                ref_path="data/EOL.xlsx",
            )
            analyzer.process() 	# ⬅ ตรงนี้ทำให้โชว์ทันที
            st.session_state["eol_analyzer"] = analyzer # ⬅ เก็บ analyzer
            st.caption(f"Using RAW file: {st.session_state.get('atten_file')}")
        except Exception as e:
            st.error(f"An error occurred during EOL analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains the attenuation report.")


elif menu == "Loss between Core":
    st.markdown("### Loss between Core")
    df_raw = st.session_state.get("atten_data") 	# ใช้ atten_data เหมือนกัน
    if df_raw is not None:
        try:
            analyzer = CoreAnalyzer(
                df_ref=None,
                df_raw_data=df_raw.copy(),
                ref_path="data/EOL.xlsx",
            )
            analyzer.process() 	# ⬅ ตรงนี้ทำให้โชว์ทันที
            st.session_state["core_analyzer"] = analyzer # ⬅ เก็บ analyzer
            st.caption(f"Using RAW file: {st.session_state.get('atten_file')}")
        except Exception as e:
            st.error(f"An error occurred during Core analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains the attenuation report.")

elif menu == "Preset status":
    st.markdown("### Preset Status (Mobaxterm Log)")
    log_txt = st.session_state.get("preset_log") # string log จาก mobaxterm
    if log_txt:
        try:
            analyzer = PresetStatusAnalyzer(log_txt)
            analyzer.process()
            render_preset_ui(analyzer)
            st.session_state["preset_analyzer"] = analyzer # ⬅ เก็บ analyzer
            st.caption(f"Using PRESET file: {st.session_state.get('preset_file')}")
        except Exception as e:
            st.error(f"An error occurred during Preset analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains the Mobaxterm/Preset log (.txt).")

elif menu == "APO Remnant":
    st.markdown("### APO Remnant Connection (APlus Log)")
    df_apo = st.session_state.get("apo_data") # DataFrame จาก APlus Log (Excel)
    log_txt = st.session_state.get("wason_log") # string log จาก WASON
    
    if df_apo is not None and log_txt is not None:
        try:
            analyzer = ApoRemnantAnalyzer(df_apo, log_txt)
            analyzer.process()
            apo_kpi(analyzer) # ฟังก์ชันแสดงผล KPI
            st.session_state["apo_analyzer"] = analyzer # ⬅ เก็บ analyzer
            st.caption(
                f"Using APLUS Log: {st.session_state.get('apo_file')} | "
                f"WASON Log: {st.session_state.get('wason_file')}"
            )
        except Exception as e:
            st.error(f"An error occurred during APO analysis: {e}")
    else:
        st.info("Please upload a ZIP file that contains both APlus Log (Excel) and WASON Log (.txt).")


elif menu == "Summary table & report":
    summary = SummaryTableReport()
    summary.render()

# ====== VISUALIZATION (ถ้ามี) ======
elif menu == "Visualization":
    render_visualization() # เรียกใช้ฟังก์ชันจาก viz.py