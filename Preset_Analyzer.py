# preset_analyzer.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple, Callable
import re
import io
import pandas as pd
import streamlit as st

# =========================
# 1) แกน Preset (Regex + Parser + Evaluator)
# =========================
# Match: [WASON][CALL 8] [30.10.90.6 30.10.10.6 85] COPPER
CALL_HEADER_RE = re.compile(r"\[WASON\]\[CALL\s+(\d+)\]\s+\[([^\]]+)\]")

# Any Conn line that contains WR
CONN_HAS_WR_RE = re.compile(r"\[WASON\]\s*\[Conn\s+\d+\].*\bWR\b", re.IGNORECASE)

# Specifically "WR NO_ALARM"
CONN_WR_NOALARM_RE = re.compile(
    r"\[WASON\]\s*\[Conn\s+\d+\][^\n]*\bWR\s+NO_ALARM\b", re.IGNORECASE
)

# A preroute line like: --2--WORK--(USED)--(SUCCESS)-- (robust to trailing text)
PREROUT_USED_RE = re.compile(
    r"\[WASON\]--\s*(\d+)\s*--\s*WORK\s*--\s*\(USED\)\s*--\s*\((\w+)\).*",
    re.IGNORECASE,
)

@dataclass
class CallBlock:
    call_id: int
    ip: str
    lines: List[str] = field(default_factory=list)

def parse_calls(text: str) -> List[CallBlock]:
    calls: List[CallBlock] = []
    cur: Optional[CallBlock] = None
    for raw in text.splitlines():
        line = raw.rstrip("\n")
        m = CALL_HEADER_RE.search(line)
        if m:
            if cur is not None:
                calls.append(cur)
            cur = CallBlock(call_id=int(m.group(1)), ip=m.group(2), lines=[])
        if cur is not None:
            cur.lines.append(line)
    if cur is not None:
        calls.append(cur)
    return calls

def evaluate_preset_status(cb: CallBlock) -> Dict[str, Any]:
    """
    Rules:
    - ต้องมี WR (Conn ที่มี WR)
    - ต้องมี 'WR NO_ALARM'
    - ใน PreRout ต้องมี 'WORK (USED) (SUCCESS)' จำนวน 1 บรรทัดพอดี
    """
    has_wr = any(CONN_HAS_WR_RE.search(ln) for ln in cb.lines)
    if not has_wr:
        return {"has_wr": False}

    wr_no_alarm = any(CONN_WR_NOALARM_RE.search(ln) for ln in cb.lines)

    used_rows = []
    for ln in cb.lines:
        m = PREROUT_USED_RE.search(ln)
        if m:
            used_rows.append({"index": int(m.group(1)), "result": m.group(2).upper(), "raw": ln})

    verdict = "FAIL"
    Restore = ""
    pr_index: Optional[int] = None

    if not wr_no_alarm:
        Restore = "WR found but not WR NO_ALARM"
    elif len(used_rows) != 1:
        Restore = f"Found {len(used_rows)} USED rows (expected 1)"
    elif used_rows[0]["result"] != "SUCCESS":
        Restore = "USED row is not SUCCESS"
    else:
        verdict = "PASS"
        pr_index = used_rows[0]["index"]
        Restore = "Normal"

    return {
        "has_wr": True,
        "wr_no_alarm": wr_no_alarm,
        "verdict": verdict,
        "Restore": Restore,
        "pr_index": pr_index,
        "used_rows": used_rows,
        "raw": "\n".join(cb.lines),
    }

# =========================
# 2) Analyzer ห่อให้ใช้ง่าย
# =========================
class PresetStatusAnalyzer:
    def __init__(
        self,
        raw_text: str,
        parse_fn: Callable[[str], List[CallBlock]] = parse_calls,
        eval_fn: Callable[[CallBlock], Dict[str, Any]] = evaluate_preset_status,
    ):
        self.raw_text = raw_text
        self.parse_fn = parse_fn
        self.eval_fn  = eval_fn

        self.calls: List[CallBlock] = []
        self.rows: List[Dict[str, Any]] = []
        self.df: pd.DataFrame | None = None
        self.summary: Dict[str, int] = {}

    def parse(self) -> List[CallBlock]:
        self.calls = list(self.parse_fn(self.raw_text))
        return self.calls

    def analyze(self) -> List[Dict[str, Any]]:
        self.rows.clear()
        for cb in self.calls:
            res = self.eval_fn(cb)
            if res and res.get("has_wr"):
                self.rows.append({
                    "Call": cb.call_id,
                    "IP": cb.ip,
                    "Preroute": res.get("pr_index"),
                    "Verdict": res.get("verdict"),
                    "Status": res.get("Restore"),
                    "Raw": res.get("raw"),
                })
        return self.rows

    def to_dataframe(self) -> Tuple[pd.DataFrame, Dict[str, int]]:
        if not self.rows:
            self.df = pd.DataFrame(columns=["Call", "IP", "Preroute", "Verdict", "Restore", "Raw"])
            self.summary = {"total": 0, "passes": 0, "fails": 0}
            return self.df, self.summary

        df = pd.DataFrame(self.rows).sort_values("Call").reset_index(drop=True)
        passes = int((df["Verdict"] == "PASS").sum())
        fails  = int((df["Verdict"] == "FAIL").sum())
        self.df = df
        self.summary = {"total": len(df), "passes": passes, "fails": fails}
        return self.df, self.summary

    @staticmethod
    def view_only(df: pd.DataFrame, only_abnormal: bool) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        return df if not only_abnormal else df[df["Verdict"] == "FAIL"]

    @staticmethod
    def export_csv_bytes(df: pd.DataFrame, drop_raw: bool = True) -> bytes:
        if df is None:
            return b""
        out_df = df.drop(columns=["Raw"]) if drop_raw and "Raw" in df.columns else df
        buf = io.StringIO()
        out_df.to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")

# =========================
# 3) UI Renderer (Streamlit)
# =========================
_CSS = """
<style>
  .kpi-row { display:grid; grid-template-columns: repeat(3,1fr); gap:.75rem; margin:.25rem 0 1rem 0;}
  .kpi-card { border:1px solid rgba(0,0,0,.06); background:#fafafa; border-radius:14px; padding:12px 14px;}
  .kpi-card .label{font-size:12px;color:#6b7280;} .kpi-card .value{font-size:24px;font-weight:700;margin-top:2px;}
  .pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;margin-right:6px;border:1px solid transparent;}
  .pill-green{background:#ecfdf5;border-color:#10b98144;} .pill-blue{background:#eff6ff;border-color:#3b82f633;}
  .pill-red{background:#fef2f2;border-color:#ef444444;}
</style>
"""

def render_preset_ui(df: pd.DataFrame, summary: Dict[str, int], only_abnormal_key: str = "preset_only_abnormal"):
    st.markdown(_CSS, unsafe_allow_html=True)

     # ---------- KPI ----------
    st.markdown("### Overall Preset Status")

    total = int(summary.get("total", 0))
    passes = int(summary.get("passes", 0))
    fails  = int(summary.get("fails", 0))

    col1, col2, col3 = st.columns(3)
    col1.metric("Preset Success", f"{passes}")
    col2.metric("Preset Abnormal", f"{fails}")
    col3.metric("Total Preset Calls", f"{total}")



    # Controls
    left, right = st.columns([1,1])
    with left:
        st.session_state.setdefault(only_abnormal_key, False)
        st.checkbox("Show only abnormal", key=only_abnormal_key)

        # ✅ เพิ่ม CSS เฉพาะ checkbox นี้
        st.markdown(f"""
        <style>
        div[data-testid="stCheckbox"][aria-labelledby="label-{only_abnormal_key}"] label {{
            font-size: 20px !important;   /* ขนาดฟอนต์ */
            font-weight: 600;             /* ตัวหนา (เลือกใส่หรือไม่ก็ได้) */
        }}
        </style>
        """, unsafe_allow_html=True)


    # Table
    view = PresetStatusAnalyzer.view_only(df, st.session_state[only_abnormal_key])
    st.dataframe(view.drop(columns=["Raw", "Verdict"], errors="ignore"), use_container_width=True, hide_index=True)

    # Per-call cards
    for _, r in view.iterrows():
        with st.container(border=True):
            call_txt = "-" if pd.isna(r.get("Call")) else int(r.Call)
            ip   = r.get("IP", "")
            st.markdown(f"**Call {call_txt}** · `{ip}`")

            pr = r.get("Preroute")
            pr_html = f'<span class="pill pill-blue">Preroute #{int(pr)}</span>' if pd.notna(pr) else ""

            if r.get("Verdict") == "PASS":
                st.success("Preset Normal")
                if pd.notna(pr):
                    st.write(f"Call {call_txt} [{ip}] uses preroute #{int(pr)}")
                st.markdown(
                    '<span class="pill pill-green">WR NO_ALARM</span>'
                    f'{pr_html}'
                    '<span class="pill pill-green">SUCCESS</span>',
                    unsafe_allow_html=True
                )
            else:
                st.error("Preset Abnormal")
                st.write(str(r.get("Restore")))
                if pd.notna(pr):
                    st.markdown(pr_html, unsafe_allow_html=True)

            with st.expander("Show raw log"):
                st.code(str(r.get("Raw")), language="text")
