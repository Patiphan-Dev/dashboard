# app.py
import os
import io
import zipfile
import uuid
from datetime import datetime, date
import pytz

import streamlit as st
import pandas as pd
from streamlit_calendar import calendar
from supabase import create_client, Client # 👈 ต้อง import Client ด้วย

# -------------------
# CONFIG / SECRETS
# -------------------
# On Streamlit Cloud: set these in the Secrets UI (not in git)
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY")
SUPABASE_BUCKET = st.secrets.get("SUPABASE_BUCKET")

if not (SUPABASE_URL and SUPABASE_KEY and SUPABASE_BUCKET):
    st.error("Please set SUPABASE_URL, SUPABASE_KEY and SUPABASE_BUCKET in Streamlit secrets.")
    st.stop()

# 👇 เพิ่ม Caching
@st.cache_resource
def init_connection(url: str, key: str) -> Client:
    """Initializes and caches the Supabase client."""
    return create_client(url, key)

# ใช้ฟังก์ชัน Caching แทนการสร้าง client โดยตรง
supabase = init_connection(SUPABASE_URL, SUPABASE_KEY) 

# -------------------
# Helpers - Supabase
# -------------------
def save_file_to_supabase(upload_date: str, file):
    """
    Upload file bytes to Supabase Storage and insert metadata into `uploads` table.
    """
    # ensure bucket exists (assume created beforehand)
    file_id = str(uuid.uuid4())
    stored_path = f"uploads/{upload_date}/{file_id}_{file.name}"

    # upload (file is UploadedFile from streamlit -> has .getvalue())
    try:
        # supabase-py storage API: upload(path, file)
        resp = supabase.storage.from_(SUPABASE_BUCKET).upload(stored_path, file.getvalue())
    except Exception as e:
        st.error(f"Storage upload error: {e}")
        return False

    # insert metadata to 'uploads' table
    payload = {
        "id": file_id,
        "upload_date": upload_date,
        "orig_filename": file.name,
        "stored_path": stored_path,
        "created_at": datetime.now().isoformat()
    }
    try:
        res = supabase.table("uploads").insert(payload).execute()
        if res.error:
            st.error(f"DB insert error: {res.error.message if hasattr(res.error,'message') else res.error}")
            return False
    except Exception as e:
        st.error(f"DB insert exception: {e}")
        return False

    return True


def list_files_by_date(upload_date: str):
    """
    Return list of tuples (id, orig_filename, stored_path) for given upload_date
    """
    try:
        res = supabase.table("uploads").select("id, orig_filename, stored_path").eq("upload_date", upload_date).order("created_at", desc=True).execute()
        rows = res.data or []
        return [(r["id"], r["orig_filename"], r["stored_path"]) for r in rows]
    except Exception as e:
        st.error(f"Error listing files: {e}")
        return []


def delete_file(file_id: str):
    """
    Delete metadata row and remove file from Storage (best-effort).
    """
    try:
        # get stored_path
        res = supabase.table("uploads").select("stored_path").eq("id", file_id).limit(1).execute()
        rows = res.data or []
        if rows:
            stored_path = rows[0]["stored_path"]
            try:
                supabase.storage.from_(SUPABASE_BUCKET).remove([stored_path])
            except Exception:
                # ignore storage errors, continue to delete metadata
                pass

        # delete metadata
        supabase.table("uploads").delete().eq("id", file_id).execute()
        return True
    except Exception as e:
        st.error(f"Delete error: {e}")
        return False


def list_dates_with_files():
    """
    Return list of tuples (upload_date, count)
    We'll fetch all upload_date rows then group in pandas to be safe.
    """
    try:
        res = supabase.table("uploads").select("upload_date").order("upload_date", desc=True).execute()
        data = res.data or []
        if not data:
            return []
        df = pd.DataFrame(data)
        grouped = df.groupby("upload_date").size().reset_index(name="count")
        rows = [(r["upload_date"], int(r["count"])) for _, r in grouped.iterrows()]
        return rows
    except Exception as e:
        st.error(f"Error listing dates: {e}")
        return []


# -------------------
# ZIP PARSER (same logic as before)
# -------------------
KW = {
    "cpu": ("cpu",),
    "fan": ("fan",),
    "msu": ("msu",),
    "client": ("client", "client board"),
    "line": ("line", "line board"),
    "wason": ("wason", "log"),
    "osc": ("osc", "osc optical"),
    "fm": ("fm", "alarm", "fault management"),
    "atten": ("optical attenuation report", "optical_attenuation_report"),
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
    # prioritize wason if txt
    if "wason" in hits and name.lower().endswith(".txt"):
        return "wason"
    return hits[0] if hits else None

def find_in_zip(zip_bytes_io):
    found = {k: None for k in KW}
    def walk(zf):
        for name in zf.namelist():
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
            if not ext or not kind or found.get(kind):
                continue
            try:
                with zf.open(name) as f:
                    df = LOADERS[ext](f)
                if kind == "wason":
                    found[kind] = (df, name)
                else:
                    found[kind] = (df, name)
            except Exception:
                continue
    walk(zipfile.ZipFile(zip_bytes_io))
    return found

# -------------------
# UI
# -------------------
st.set_page_config(layout="wide", page_title="DWDM Monitoring (Supabase)")
st.title("DWDM Monitoring Dashboard (Supabase Storage)")

menu = st.sidebar.radio("เมนู", ["หน้าแรก","CPU","FAN","MSU","Line board","Client board","Fiber Flapping","Loss between Core","Loss between EOL","Summary"])

if menu == "หน้าแรก":
    st.subheader("Upload ZIP files to Supabase")
    chosen_date = st.date_input("Select date", value=date.today())
    files = st.file_uploader("Upload ZIP files (.zip)", type=["zip"], accept_multiple_files=True)

    # keep pending files across reruns
    if files:
        st.session_state["pending_files"] = files

    if st.button("Upload") and st.session_state.get("pending_files"):
        uploaded = st.session_state.get("pending_files")
        success_count = 0
        for f in uploaded:
            ok = save_file_to_supabase(str(chosen_date), f)
            if ok:
                success_count += 1
        st.success(f"Uploaded {success_count}/{len(uploaded)} files")
        # clear pending
        st.session_state.pop("pending_files", None)
        st.experimental_rerun()

    # Calendar: show dates that have files
    events = []
    for d, cnt in list_dates_with_files():
        events.append({"title": f"{cnt} file(s)", "start": d, "allDay": True, "color": "blue"})
    calendar_res = calendar(events=events, options={"initialView": "dayGridMonth","height":"400px","selectable":True}, key="calendar")

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
        selected = []
        for fid, fname, fpath in files_list:
            col1, col2 = st.columns([4,1])
            with col1:
                checked = st.checkbox(fname, key=f"chk_{fid}")
                if checked:
                    selected.append((fid, fname, fpath))
            with col2:
                if st.button("Delete", key=f"del_{fid}"):
                    delete_file(fid)
                    st.experimental_rerun()

        if st.button("Run Analysis"):
            if not selected:
                st.warning("Select at least one file")
            else:
                # download and parse
                total = 0
                for fid, fname, fpath in selected:
                    with st.spinner(f"Downloading {fname}..."):
                        try:
                            # supabase download returns bytes
                            file_bytes = supabase.storage.from_(SUPABASE_BUCKET).download(fpath)
                            z = io.BytesIO(file_bytes)
                            res = find_in_zip(z)
                        except Exception as e:
                            st.error(f"Error downloading/processing {fname}: {e}")
                            continue

                        # push to session_state
                        for kind, pack in res.items():
                            if not pack:
                                continue
                            df, zname = pack
                            st.session_state[f"{kind}_data"] = df
                            st.session_state[f"{kind}_file"] = zname
                        total += 1
                st.success(f"Analysis finished for {total} file(s)")

# -------------------
# Analyzer pages (simple rendering of loaded DataFrames)
# -------------------
elif menu == "CPU":
    df = st.session_state.get("cpu_data")
    if df is not None:
        st.header("CPU Analysis")
        st.write("Using:", st.session_state.get("cpu_file"))
        st.dataframe(df.head(50))
    else:
        st.info("Upload a ZIP containing a CPU file and run analysis first.")

elif menu == "FAN":
    df = st.session_state.get("fan_data")
    if df is not None:
        st.header("FAN Analysis")
        st.dataframe(df.head(50))
    else:
        st.info("Upload a ZIP containing a FAN file and run analysis first.")

elif menu == "MSU":
    df = st.session_state.get("msu_data")
    if df is not None:
        st.header("MSU Analysis")
        st.dataframe(df.head(50))
    else:
        st.info("Upload a ZIP containing MSU and run analysis first.")

elif menu == "Line board":
    df = st.session_state.get("line_data")
    if df is not None:
        st.header("Line board")
        st.dataframe(df.head(50))
    else:
        st.info("Upload ZIP with Line sheet and run analysis first.")

elif menu == "Client board":
    df = st.session_state.get("client_data")
    if df is not None:
        st.header("Client board")
        st.dataframe(df.head(50))
    else:
        st.info("Upload ZIP with Client sheet and run analysis first.")

elif menu == "Fiber Flapping":
    st.info("Fiber Flapping: show OSC & FM if available")
    osc = st.session_state.get("osc_data")
    fm = st.session_state.get("fm_data")
    if osc is not None:
        st.subheader("OSC")
        st.dataframe(osc.head(50))
    if fm is not None:
        st.subheader("FM")
        st.dataframe(fm.head(50))

elif menu == "Loss between Core" or menu == "Loss between EOL":
    df = st.session_state.get("atten_data")
    if df is not None:
        st.dataframe(df.head(50))
    else:
        st.info("Upload ZIP with attenuation report and run analysis first.")

elif menu == "Summary":
    st.header("Summary (mock)")
    st.write("You can implement summary aggregation here (e.g. read analyzers from session state)")

# Debug helper
if st.sidebar.checkbox("🔍 Debug"):
    st.sidebar.write("Pending files:", "Yes" if st.session_state.get("pending_files") else "No")
    st.sidebar.write("Selected date:", st.session_state.get("selected_date"))
    st.sidebar.write("Dates with files:", list_dates_with_files())
