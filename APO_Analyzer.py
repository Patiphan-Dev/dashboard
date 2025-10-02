from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set
import re
import html

import streamlit as st
import pandas as pd
import plotly.express as px



@dataclass
class _SiteBucket:
    name: str
    wason_lines: List[str] = field(default_factory=list)
    apop_lines: List[str] = field(default_factory=list)
    # (traffic_hex, conn_hex, state, raw_line)
    apop_rows: List[Tuple[str, str, str, str]] = field(default_factory=list)


class ApoRemnantAnalyzer:
    def __init__(self, raw_text: str, site_map: Dict[str, str] | None = None):
        """
        raw_text: เนื้อ log ทั้งไฟล์ (string)
        site_map: map ip → ชื่อไซต์ (ไม่ส่งมาก็มีค่า default ให้)
        """
        self.raw_text = raw_text
        self.lines = raw_text.splitlines()
        self.site_map = site_map or {
            "30.10.90.6":  "HYI-4",
            "30.10.10.6":  "Jasmine",
            "30.10.30.6":  "Phu Nga",
            "30.10.50.6":  "SNI-POI",
            "30.10.70.6":  "NKS",
            "30.10.110.6": "PKT",
        }

        # ===== regex =====
        self.re_wason_exec = re.compile(r'^\s*ZXPOTN\(.*\)#\s*exec\s+diag_c\("cc-cmd setcallcv SetupApo"\)')
        self.re_wason_end  = re.compile(r'^\[WASON\]ushell command finished\b', re.I)
        self.re_wason_conn = re.compile(r"^\[WASON\]\s*Conn\s*\[")

        self.re_apop_begin = re.compile(r'^\[APOPLUS\]\s*===\s*show all och-inst\s*===', re.I)
        self.re_apop_top   = re.compile(r'^\[APOPLUS\]\s*TopNeIp\s*:\s*([0-9\.]+)')
        self.re_apop_end   = re.compile(r'^\[APOPLUS\]ushell command finished\b', re.I)
        self.re_apop_row   = re.compile(
            r"^\[APOPLUS\]\d+\s+0x[0-9a-fA-F]+\s+0x[0-9a-fA-F]+\s+(0x[0-9a-fA-F]{8})\s+(0x[0-9a-fA-F]{8}).*(HEAD_[A-Z_]+)"
        )

        # outputs
        self.per_site: Dict[str, _SiteBucket] = {}  # key: wason_first_ip
        # [(ip, (site_name, wason_snip, apop_snip, to_red_set), has_mismatch, site_name_for_sort)]
        self.rendered: List[Tuple[str, Tuple[str, str, str, Set[str]], bool, str]] = []

    # ---------- helpers ----------
    @staticmethod
    def _topne_to_wason_ip(top_ne_ip: str) -> Optional[str]:
        m = re.match(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", top_ne_ip)
        if not m:
            return None
        x = m.group(3)
        return f"30.10.{x}.6"

    @staticmethod
    def _wason_pair_for_compare(conn_line: str) -> Optional[Tuple[str, int, str]]:
        """
        [WASON] Conn [30.10.x.x 30.10.y.y CALLID CONNNO] ...
        return: (first_ip, call_id:int, conn_hex:str)
        """
        m = re.search(r"Conn\s*\[\s*([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s*\]", conn_line)
        if not m:
            return None
        first_ip, _second_ip, call_id_str, conn_no_str = m.groups()
        call_id = int(call_id_str)
        conn_hex = f"0x{int(conn_no_str):08x}".lower()
        return first_ip, call_id, conn_hex

    def _ensure_bucket(self, ip: str):
        if ip not in self.per_site:
            self.per_site[ip] = _SiteBucket(name=self.site_map.get(ip, ip))

    # ---------- ขั้นที่ 1: parse ----------
    def parse(self) -> Dict[str, _SiteBucket]:
        wason_prebuf: List[str] = []
        apop_prebuf:  List[str] = []
        cap_wason = cap_apop = False
        cur_wason_ip_ctx: Optional[str] = None
        cur_apop_site_ip: Optional[str] = None

        for ln in self.lines:
            # WASON begin / end
            if self.re_wason_exec.search(ln):
                cap_wason = True
                cur_wason_ip_ctx = None
                wason_prebuf.clear()
                wason_prebuf.append(ln)
            if self.re_wason_end.search(ln):
                if cap_wason and cur_wason_ip_ctx:
                    self.per_site[cur_wason_ip_ctx].wason_lines.append(ln)
                cap_wason = False
                cur_wason_ip_ctx = None
                wason_prebuf.clear()
                continue

            # APOP begin / end
            if self.re_apop_begin.search(ln):
                cap_apop = True
                cur_apop_site_ip = None
                apop_prebuf.clear()
                apop_prebuf.append(ln)
                continue
            if self.re_apop_end.search(ln):
                if cap_apop and cur_apop_site_ip:
                    self.per_site[cur_apop_site_ip].apop_lines.append(ln)
                cap_apop = False
                cur_apop_site_ip = None
                apop_prebuf.clear()
                continue

            # collect WASON
            if cap_wason:
                if ln.startswith("[WASON]"):
                    if cur_wason_ip_ctx is None:
                        wason_prebuf.append(ln)
                        if self.re_wason_conn.search(ln):
                            info = self._wason_pair_for_compare(ln)
                            if info:
                                first_ip, _, _ = info
                                cur_wason_ip_ctx = first_ip
                                self._ensure_bucket(first_ip)
                                self.per_site[first_ip].wason_lines.extend(wason_prebuf)
                                wason_prebuf.clear()
                    else:
                        self.per_site[cur_wason_ip_ctx].wason_lines.append(ln)
                continue

            # collect APOP
            if cap_apop and ln.startswith("[APOPLUS]"):
                if cur_apop_site_ip is None:
                    apop_prebuf.append(ln)
                    mtop = self.re_apop_top.search(ln)
                    if mtop:
                        mapped_ip = self._topne_to_wason_ip(mtop.group(1))
                        cur_apop_site_ip = mapped_ip
                        if mapped_ip:
                            self._ensure_bucket(mapped_ip)
                            self.per_site[mapped_ip].apop_lines.extend(apop_prebuf)
                            apop_prebuf.clear()
                    continue

                self.per_site[cur_apop_site_ip].apop_lines.append(ln)
                mrow = self.re_apop_row.match(ln)
                if mrow:
                    traffic = mrow.group(1).lower()
                    connno  = mrow.group(2).lower()
                    state   = mrow.group(3)
                    self.per_site[cur_apop_site_ip].apop_rows.append((traffic, connno, state, ln))
                continue

        return self.per_site

    # ---------- ขั้นที่ 2: analyze ----------
    @staticmethod
    def _traffic_hex_from(call_id: int, scheme: str) -> str:
        return f"0x{(call_id << 24):08x}" if scheme == "shifted" else f"0x{call_id:08x}"


    def analyze(self):
        self.rendered.clear()

        for wip, bucket in self.per_site.items():
            site_name     = bucket.name
            wason_snippet = "\n".join(bucket.wason_lines)
            apop_snippet  = "\n".join(bucket.apop_lines)

            # --- index APOP: เอาเฉพาะ HEAD_DETECT_WAITING ---
            apop_by_traffic: Dict[str, Dict[str, str]] = {}
            valid_states = {"HEAD_DETECT_WAITING", "HEAD_ERROR_DETECTING"}
            for t, c, state, ln_ap in bucket.apop_rows:
                if state in valid_states:
                    apop_by_traffic.setdefault(t, {})[c] = ln_ap


            # --- collect WASON calls ---
            wason_calls: List[Tuple[int, str, str]] = []  # (call_id, conn_hex, raw_line)
            for ln_w in bucket.wason_lines:
                if not self.re_wason_conn.search(ln_w):
                    continue
                parsed = self._wason_pair_for_compare(ln_w)
                if not parsed:
                    continue
                first_ip, call_id, c_hex = parsed
                if first_ip == wip:
                    wason_calls.append((call_id, c_hex, ln_w))

            if not wason_calls:
                self.rendered.append((wip, (site_name, wason_snippet, apop_snippet, set(), set()), False, site_name))
                continue

            # --- เลือก scheme ---
            def score_scheme(scheme: str) -> int:
                return sum(self._traffic_hex_from(call_id, scheme) in apop_by_traffic for call_id, _c, _l in wason_calls)

            score_shifted = score_scheme("shifted")
            score_direct  = score_scheme("direct")
            if score_shifted == score_direct:
                shifted_like = sum(t.endswith("000000") for t in apop_by_traffic.keys())
                scheme = "shifted" if shifted_like > 0 else "direct"
            else:
                scheme = "shifted" if score_shifted > score_direct else "direct"

            # --- compare Conn แบบ symmetric ---
            to_red_apop: Set[str] = set()
            to_red_wason: Set[str] = set()
            seen_apop_keys: Set[Tuple[str, str]] = set()  # (traffic_hex, conn_hex) ที่ WASON เช็คแล้ว

            for call_id, c_hex, ln_wason in wason_calls:
                t_hex = self._traffic_hex_from(call_id, scheme)
                apop_conns = apop_by_traffic.get(t_hex, {})

                if c_hex in apop_conns:
                    # ✅ match → ไม่ทำอะไร
                    seen_apop_keys.add((t_hex, c_hex))
                else:
                    # ❌ mismatch → แดงทั้งคู่
                    to_red_wason.add(ln_wason)
                    for ap_ln in apop_conns.values():
                        to_red_apop.add(ap_ln)

            # --- orphan APOP (Conn ที่มีใน APOP แต่ไม่เจอใน WASON เลย) ---
            for t_hex, conns in apop_by_traffic.items():
                for c_hex, ap_ln in conns.items():
                    if (t_hex, c_hex) not in seen_apop_keys:
                        to_red_apop.add(ap_ln)
                        # ✅ symmetric: mark WASON ทั้งหมดที่ traffic ตรงนี้
                        for call_id, c_hex_w, ln_w in wason_calls:
                            if self._traffic_hex_from(call_id, scheme) == t_hex:
                                to_red_wason.add(ln_w)

            has_mismatch = bool(to_red_apop or to_red_wason)
            self.rendered.append(
                (wip, (site_name, wason_snippet, apop_snippet, to_red_wason, to_red_apop), has_mismatch, site_name)
            )

        return self.rendered






    # ---------- ขั้นที่ 3: render ----------
    def render_streamlit(self, view_choice: Optional[str] = None, display_fn=None):
        self._inject_css()

        if view_choice == "APO":
            to_show = [x for x in self.rendered if x[2]]
        elif view_choice == "No APO":
            to_show = [x for x in self.rendered if not x[2]]
        else:
            to_show = self.rendered

        if not to_show:
            st.info("No data to display")
            return

        to_show.sort(key=lambda x: x[3])
        display = display_fn or self.display_logs_separate
        for _, args, _, _ in to_show:
            site_name, wason_snip, apop_snip, to_red_wason, to_red_apop = args
            display(site_name, wason_snip, apop_snip, to_red_wason, to_red_apop)



    # --- renderer ของแต่ละไซต์ ---
    def display_logs_separate(self, site_name: str, wason_text: str, apop_text: str,
                            wason_lines_to_red: Set[str], apop_lines_to_red: Set[str]):
        wason_lines = wason_text.splitlines() if wason_text else ["<i>No WASON log</i>"]
        apop_lines  = apop_text.splitlines()  if apop_text  else ["<i>No APOP log</i>"]

        wason_rows = []
        for wl in wason_lines:
            wl_html = wl if wl.startswith("<i>") else html.escape(wl)
            ws_cls = ' class="mismatch"' if wl in wason_lines_to_red else ""
            wason_rows.append(f"<tr><td{ws_cls}>{wl_html}</td></tr>")

        apop_rows = []
        for al in apop_lines:
            al_html = al if al.startswith("<i>") else html.escape(al)
            ap_cls = ' class="mismatch"' if al in apop_lines_to_red else ""
            apop_rows.append(f"<tr><td{ap_cls}>{al_html}</td></tr>")

        site_name = html.escape(site_name)
        html_block = f"""
        <div class="site-header"><div class="pill">{site_name}</div></div>
        <div class="grid-2col">
        <div class="pane">
            <div class="title">WASON</div>
            <div class="log-table-container">
            <table class="log-table"><tbody>{''.join(wason_rows)}</tbody></table>
            </div>
        </div>
        <div class="pane">
            <div class="title">APOPLUS</div>
            <div class="log-table-container">
            <table class="log-table"><tbody>{''.join(apop_rows)}</tbody></table>
            </div>
        </div>
        </div>
        """
        st.markdown(html_block, unsafe_allow_html=True)



    # --- CSS สำหรับ layout ---
    @staticmethod
    def _inject_css():
        LOG_CSS = """
        <style>
        .grid-2col{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:8px 0 22px;}
        .pane{border:1px solid #dcdcdc;border-radius:6px;background:#fff;}
        .title{font-weight:600;padding:6px 8px;border-bottom:1px solid #e7e7e7;background:#fafafa;color:#000;}
        .log-table-container{max-height:70vh;overflow:auto;background:#fff;border-radius:6px;}
        .log-table{width:100%;border-collapse:collapse;font-family:ui-monospace, Menlo, Consolas, monospace;font-size:13px;table-layout:fixed;}
        .log-table th, .log-table td{padding:4px 8px;border-bottom:1px solid #eee;vertical-align:top;text-align:left;white-space:pre;word-wrap:break-word;}
        .log-table thead th{position:sticky; top:0;background:#fafafa;z-index:1;}
        td.mismatch{background:#fee2e2;}
        .site-header{display:flex;align-items:center;gap:8px;margin:10px 0 6px;}
        .pill{background:#1f2937;color:#e5e7eb;padding:3px 8px;border-radius:999px;font-weight:600;font-size:12px}
        </style>
        """
        st.markdown(LOG_CSS, unsafe_allow_html=True)



# =========================
# Helper: KPI summary
# =========================

def apo_kpi(rendered: list[tuple]):
    """
    rendered: list ที่ได้จาก ApoRemnantAnalyzer.analyze()
    """
    # ---------- Summary Count ----------
    total_sites = len(rendered)
    apo_sites   = sum(1 for x in rendered if x[2])   # has_mismatch = True
    noapo_sites = sum(1 for x in rendered if not x[2])  # has_mismatch = False

    # ---------- KPI Cards ----------
    st.markdown("### APO Remnant Status Overview")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Sites", f"{total_sites}")
    with col2:
        st.metric("No APO Remnant", f"{noapo_sites}")
    with col3:
        st.metric("APO Remnant", f"{apo_sites}")

    # ---------- Donut Chart ----------
    df_summary = pd.DataFrame({
        "Status": (["No APO Remnant"] * noapo_sites) + (["APO Remnant"] * apo_sites)
    })

    fig = px.pie(
        df_summary,
        names="Status",
        hole=0.5,
        color="Status",
        color_discrete_map={
            "No APO Remnant": "green",
            "APO Remnant": "red",
        }
    )
    fig.update_traces(textinfo="value+label")
    fig.add_annotation(
        dict(
            text=f"Total<br>{total_sites}",
            x=0.5, y=0.5,
            font_size=18,
            showarrow=False,
            xanchor="center",
            yanchor="middle",
            font_color="black"
        )
    )
    st.plotly_chart(fig, use_container_width=True)
