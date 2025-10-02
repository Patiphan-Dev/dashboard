import os
import uuid
from datetime import datetime, date
import pytz
import streamlit as st
from streamlit_calendar import calendar
import io, zipfile
import pandas as pd
from supabase import create_client  # SDK 2.x

# ====== IMPORT ANALYZERS ======
from CPU_Analyzer import CPU_Analyzer
from FAN_Analyzer import FAN_Analyzer
from MSU_Analyzer import MSU_Analyzer
from Line_Analyzer import Line_Analyzer
from Client_Analyzer import Client_Analyzer
from Fiberflapping_Analyzer import FiberflappingAnalyzer
from EOL_Core_Analyzer import EOLAnalyzer, CoreAnalyzer
from Preset_Analyzer import PresetStatusAnalyzer, render_preset_ui
from APO_Analyzer import ApoRemnantAnalyzer, apo_kpi
from viz import render_visualization
from table1 import SummaryTableReport

# ====== CONFIG ======
st.set_page_config(layout="wide")
pd.set_option("styler.render.max_elements", 1_200_000)

# ==========================================================
# ====== SUPABASE INIT ======
# ==========================================================
try:
    SUPABASE_BUCKET = st.secrets["SUPABASE_BUCKET"]
except KeyError:
    st.error("Error: Please set SUPABASE_BUCKET in .streamlit/secrets.toml")
    st.stop()

@st.cache_resource
def get_supabase_client():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = get_supabase_client()

# ==========================================================
# ====== SUPABASE FUNCTIONS ======
# ==========================================================
def save_file(upload_date: str, file):
    stored_name = f"{uuid.uuid4()}_{file.name}"
    file_bytes = file.read()
    try:
        supabase.storage.from_(SUPABASE_BUCKET).upload(stored_name, file_bytes)
        st.success(f"Uploaded {file.name} to bucket {SUPABASE_BUCKET}")
    except Exception as e:
        st.error(f"Failed to upload {file.name}: {e}")
        return
    
    supabase.table("uploads").insert({
        "upload_date": upload_date,
        "orig_filename": file.name,
        "stored_path": stored_name,
        "created_at": datetime.now().isoformat()
    }).execute()
    
def debug_file_status(file_id, stored_name):
    res = supabase.table("uploads").select("*").eq("id", file_id).execute()
    in_table = bool(res.data)
    
    files_in_bucket = supabase.storage.from_(SUPABASE_BUCKET).list()
    in_storage = any(f['name'] == stored_name for f in files_in_bucket)
    
    st.write(f"File {file_id}: in_table={in_table}, in_storage={in_storage}")
    return in_table and in_storage

def list_files_by_date(upload_date: str):
    res = supabase.table("uploads").select("id, orig_filename, stored_path").eq("upload_date", upload_date).execute()
    if not res.data:
        return []
    return [(r['id'], r['orig_filename'], r['stored_path']) for r in res.data]

def delete_file(file_id: str):
    res = supabase.table("uploads").select("stored_path").eq("id", file_id).limit(1).execute()
    if res.data:
        stored_path = res.data[0]['stored_path']
        try:
            supabase.storage.from_(SUPABASE_BUCKET).remove([stored_path])
        except:
            pass
    supabase.table("uploads").delete().eq("id", file_id).execute()
    st.rerun()

def list_dates_with_files():
    res = supabase.table("uploads").select("upload_date").order("upload_date", desc=True).execute()
    if not res.data:
        return []
    df = pd.DataFrame(res.data)
    grouped = df.groupby("upload_date").size().reset_index(name="count")
    return [(row["upload_date"], int(row["count"])) for _, row in grouped.iterrows()]
 print("DEBUG list_dates_with_files res:", res.data)
# ==========================================================
# ====== CLEAR SESSION ======
# ==========================================================
def clear_all_uploaded_data():
    st.session_state.clear()

# ==========================================================
# ====== ZIP PARSER ======
# ==========================================================
KW = {
    "cpu": ("cpu",),
    "fan": ("fan",),
    "msu": ("msu",),
    "client": ("client", "client board"),
    "line": ("line","line board"),
    "wason": ("wason","log"),
    "osc": ("osc","osc optical"),
    "fm": ("fm","alarm","fault management"),
    "atten": ("optical attenuation report","optical_attenuation_report","optical attenuation"),
    "preset": ("mobaxterm", "moba xterm", "moba"),
    "apo": ("aplus", "aplus log"),
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
    if "wason" in hits and n.endswith(".txt"): return "wason"
    if "preset" in hits and n.endswith(".txt"): return "preset"
    if "apo" in hits and (n.endswith(".xlsx") or n.endswith(".xls")): return "apo"
    if "line" in hits and (n.endswith(".xlsx") or n.endswith(".xls") or n.endswith(".xlsm")): return "line"
    for k in ("fan","cpu","msu","client","osc","fm","atten"):
        if k in hits: return k
    return hits[0] if hits else None

def find_in_zip(zip_file):
    found = {k: None for k in KW}
    def walk(zf):
        for name in zf.namelist():
            if name.endswith("/"): continue
            lname = name.lower()
            if lname.endswith(".zip"):
                try: walk(zipfile.ZipFile(io.BytesIO(zf.read(name))))
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
    walk(zipfile.ZipFile(zip_file))
    return found

def safe_copy(obj):
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    return obj

# ==========================================================
# ====== SIDEBAR ======
# ==========================================================
menu = st.sidebar.radio("เลือกกิจกรรม", [
    "หน้าแรก","Visualization","CPU","FAN","MSU","Line board","Client board",
    "Fiber Flapping","Loss between Core","Loss between EOL","Preset status","APO Remnant","Summary table & report"
])

# ==========================================================
# ====== หน้าแรก (Upload + Calendar + Run Analysis + Delete) ======
# ==========================================================
if menu == "หน้าแรก":
    st.subheader("DWDM Monitoring Dashboard")
    st.markdown("#### Upload & Manage ZIP Files (Cloud Storage)")

    chosen_date = st.date_input("Select date", value=date.today())
    files = st.file_uploader("Upload ZIP files", type=["zip"], accept_multiple_files=True, key=f"uploader_{chosen_date}")
    
    if files and st.button("Upload", key=f"upload_btn_{chosen_date}"):
        for file in files:
            save_file(str(chosen_date), file)
        st.rerun()

    st.subheader("Calendar")
    events = [{"title": f"{cnt} file(s)", "start": d, "allDay": True, "color": "blue"} for d, cnt in list_dates_with_files()]
    calendar_res = calendar(events=events, options={"initialView":"dayGridMonth","height":"400px","selectable":True}, key="calendar")

    if "selected_date" not in st.session_state:
        st.session_state["selected_date"] = str(date.today())

    clicked_date = None
    if calendar_res and calendar_res.get("callback") == "dateClick":
        dt_utc = datetime.fromisoformat(calendar_res["dateClick"]["date"].replace("Z","+00:00"))
        clicked_date = dt_utc.astimezone(pytz.timezone("Asia/Bangkok")).date().isoformat()

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
            col1, col2 = st.columns([4,1])
            with col1:
                checked = st.checkbox(fname, key=f"chk_{fid}")
                if checked: selected_files.append((fid, fname, fpath))
            with col2:
                if st.button("Delete", key=f"del_{fid}"):
                    delete_file(fid)

        if st.button("Run Analysis", key="analyze_btn"):
            if not selected_files:
                st.warning("Please select at least one file to analyze")
            else:
                clear_all_uploaded_data()
                total = 0
                for fid, fname, fpath in selected_files:
                    try:
                        with st.spinner(f"Downloading and processing {fname}..."):
                            file_bytes = supabase.storage.from_(SUPABASE_BUCKET).download(fpath)
                            zip_bytes = io.BytesIO(file_bytes)
                            res = find_in_zip(zip_bytes)
                        st.success(f"Processed {fname}")
                    except Exception as e:
                        st.error(f"Error processing {fname}: {e}")
                        continue
                    for kind, pack in res.items():
                        if not pack: continue
                        df, zname = pack
                        if kind in ("wason", "preset"):
                            st.session_state[f"{kind}_log"] = df
                            st.session_state[f"{kind}_file"] = zname
                        else:
                            st.session_state[f"{kind}_data"] = df
                            st.session_state[f"{kind}_file"] = zname
                    total += 1
                st.session_state["zip_loaded"] = True
                st.success(f"✅ Analysis finished for {total} selected file(s)")

# ==========================================================
# ====== Analyzer Pages ======
# ==========================================================
elif menu == "CPU":
    if st.session_state.get("zip_loaded"):
        CPU_Analyzer(st.session_state.get("cpu_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "FAN":
    if st.session_state.get("zip_loaded"):
        FAN_Analyzer(st.session_state.get("fan_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "MSU":
    if st.session_state.get("zip_loaded"):
        MSU_Analyzer(st.session_state.get("msu_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Line board":
    if st.session_state.get("zip_loaded"):
        Line_Analyzer(st.session_state.get("line_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Client board":
    if st.session_state.get("zip_loaded"):
        Client_Analyzer(st.session_state.get("client_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Fiber Flapping":
    if st.session_state.get("zip_loaded"):
        FiberflappingAnalyzer(st.session_state.get("cpu_data"), st.session_state.get("fan_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Loss between Core":
    if st.session_state.get("zip_loaded"):
        CoreAnalyzer(st.session_state.get("cpu_data"), st.session_state.get("msu_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Loss between EOL":
    if st.session_state.get("zip_loaded"):
        EOLAnalyzer(st.session_state.get("msu_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Preset status":
    if st.session_state.get("zip_loaded"):
        render_preset_ui(st.session_state.get("preset_log"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "APO Remnant":
    if st.session_state.get("zip_loaded"):
        ApoRemnantAnalyzer(st.session_state.get("apo_data"))
        apo_kpi(st.session_state.get("apo_data"))
    else:
        st.info("Please upload & analyze ZIP files first.")

elif menu == "Summary table & report":
    if st.session_state.get("zip_loaded"):
        SummaryTableReport(st.session_state)
    else:
        st.info("Please upload & analyze ZIP files first.")
