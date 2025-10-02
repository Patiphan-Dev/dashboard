import streamlit as st
import pandas as pd
from typing import Optional
from report import generate_report


from FAN_Analyzer import FAN_Analyzer
from CPU_Analyzer import CPU_Analyzer
from MSU_Analyzer import MSU_Analyzer
from Line_Analyzer import Line_Analyzer
from Client_Analyzer import Client_Analyzer
from Fiberflapping_Analyzer import FiberflappingAnalyzer
from EOL_Core_Analyzer import EOLAnalyzer, CoreAnalyzer

# ==============================
# Helper: auto-create analyzer
# ==============================
def _ensure_analyzer(key: str, analyzer_cls, ref_file: str, ns: str):
    """
    ตรวจสอบและสร้าง analyzer อัตโนมัติถ้ายังไม่มี
    key = 'cpu' หรือ 'fan' หรือ 'msu' หรือ 'line' หรือ 'client' หรือ 'eol' หรือ 'core'
    """
    analyzer_key = f"{key}_analyzer"
    data_key = f"{key}_data"

    if st.session_state.get(analyzer_key) is None and st.session_state.get(data_key) is not None:
        try:
            df_ref = pd.read_excel(ref_file)

            if key == "cpu":
                analyzer = analyzer_cls(
                    df_cpu=st.session_state[data_key].copy(),
                    df_ref=df_ref.copy(),
                    ns=ns
                )
            elif key == "fan":
                analyzer = analyzer_cls(
                    df_fan=st.session_state[data_key].copy(),
                    df_ref=df_ref.copy(),
                    ns=ns
                )
            elif key == "msu":
                analyzer = analyzer_cls(
                    df_msu=st.session_state[data_key].copy(),
                    df_ref=df_ref.copy(),
                    ns=ns
                )
            elif key == "line":
                analyzer = analyzer_cls(
                    df_line=st.session_state[data_key].copy(),
                    df_ref=df_ref.copy(),
                    ns=ns
                )
            elif key == "client":
                analyzer = analyzer_cls(
                    df_client=st.session_state[data_key].copy(),
                    ref_path=ref_file
                )
            else:
                return

            analyzer.prepare()  # ✅ ใช้ prepare() (ไม่ render UI)
            st.session_state[analyzer_key] = analyzer

            st.write(
                f"DEBUG: Analyzer {key} created. "
                f"df_abnormal rows = {len(analyzer.df_abnormal) if analyzer.df_abnormal is not None else 'None'}"
            )
        except Exception as e:
            st.warning(f"Auto-create {key.upper()} analyzer failed: {e}")



# ==============================
# Styler Helper
# ==============================
#def _style_abnormal_table(df_abn: pd.DataFrame, value_col: str) -> "pd.io.formats.style.Styler":
    #"""ไฮไลต์ abnormal column (df_abn เป็น abnormal rows อยู่แล้ว)"""
    #def highlight_red(_):
        #return "background-color:#ff9999; color:black"
    #return df_abn.style.applymap(highlight_red, subset=[value_col])


# ==============================
# SummaryTableReport (รวมทุก Analyzer)
# ==============================
class SummaryTableReport:
    """Summary Table & Report รวมทุก Analyzer"""

    def __init__(self):
        self.sections = []  # เก็บ summary ของแต่ละ analyzer

    def _get_summary(self, key: str, analyzer_cls, details: str, value_col: str):
        """ดึง analyzer จาก session และคืนค่า (status, details, df_abn, df_abn_by_type)"""
        analyzer = st.session_state.get(f"{key}_analyzer")

        if analyzer is None:
            st.write(f"DEBUG: Analyzer {key} not found in session_state")
            return ("No data", details, None, {})

        df_abn = getattr(analyzer, "df_abnormal", None)
        st.write(f"DEBUG: {key} df_abnormal type={type(df_abn)}, size={(len(df_abn) if df_abn is not None else 'None')}")
        df_abn_by_type = getattr(analyzer, "df_abnormal_by_type", {})
        status = "Normal"
        if df_abn is not None and not df_abn.empty:
            status = "Abnormal"
        elif df_abn is None:
            status = "No data"

        return (status, details, df_abn, df_abn_by_type)

    def render(self) -> None:
        st.markdown("## Summary Table — Network Inspection")

        #2 ✅ Ensure analyzers are ready
        _ensure_analyzer("cpu", CPU_Analyzer, "data/CPU.xlsx", "cpu_summary")
        _ensure_analyzer("fan", FAN_Analyzer, "data/FAN.xlsx", "fan_summary")
        _ensure_analyzer("msu", MSU_Analyzer, "data/MSU.xlsx", "msu_summary")
        _ensure_analyzer("line", Line_Analyzer, "data/Line.xlsx", "line_summary")
        _ensure_analyzer("client", Client_Analyzer, "data/Client.xlsx", "client_summary")
   






        # ===== Header =====
        col1, col2, col3, col4, col5 = st.columns([1, 1, 3, 1, 1])
        col1.markdown("**Type**")
        col2.markdown("**Task**")
        col3.markdown("**Details**")
        col4.markdown("**Results**")
        col5.markdown("**View**")

        # ==============================
        # CPU Section
        # ==============================
        #3
        CPU_DETAILS = "Threshold: Normal if ≤ 90%, Abnormal if > 90%"
        cpu_status, cpu_details, cpu_abn, cpu_abn_by_type = self._get_summary(
            "cpu", CPU_Analyzer, CPU_DETAILS, "CPU utilization ratio"
        )
        self._render_row("Performance", "CPU board", cpu_details, cpu_status, cpu_abn, "CPU utilization ratio")

        # ==============================
        # FAN Section
        # ==============================
        FAN_DETAILS = (
            "FAN ratio performance\n"
            "FCC: Normal if ≤ 120, Abnormal if > 120\n"
            "FCPP: Normal if ≤ 250, Abnormal if > 250\n"
            "FCPL: Normal if ≤ 120, Abnormal if > 120\n"
            "FCPS: Normal if ≤ 230, Abnormal if > 230"
        )
        fan_status, fan_details, fan_abn, fan_abn_by_type = self._get_summary(
            "fan", FAN_Analyzer, FAN_DETAILS, "Value of Fan Rotate Speed(Rps)"
        )
        self._render_row("Performance", "FAN board", fan_details, fan_status, fan_abn, "Value of Fan Rotate Speed(Rps)")

        # ==============================
        # MSU Section
        # ==============================
        MSU_DETAILS = "Threshold: Should remain within normal range (not high)"
        msu_status, msu_details, msu_abn, msu_abn_by_type = self._get_summary(
            "msu", MSU_Analyzer, MSU_DETAILS, "Laser Bias Current(mA)"
        )
        self._render_row("Performance", "MSU board", msu_details, msu_status, msu_abn, "Laser Bias Current(mA)")

        # ==============================
        # LINE Section
        # ==============================
        LINE_DETAILS = "Normal input/output power [xx–xx dB]"
        line_status, line_details, line_abn, line_abn_by_type = self._get_summary(
            "line", Line_Analyzer, LINE_DETAILS, "Instant BER After FEC"
        )
        self._render_row("Performance", "Line board", line_details, line_status, line_abn, "Instant BER After FEC")

        CLIENT_DETAILS = ("Normal input/output power [xx–xx dB]")
        client_status, client_details, client_abn, client_abn_by_type = self._get_summary(
            "client", Client_Analyzer, CLIENT_DETAILS, "Input Optical Power(dBm)"
        )
        self._render_row("Performance", "Client board", client_details, client_status, client_abn, "Input Optical Power(dBm)")

        # ==============================
        # FLAPPING Section
        # ==============================
        FLAP_DETAILS = "Threshold: Normal if ≤ 2 dB, Abnormal if > 2 dB"
        fiber_status, fiber_details, fiber_abn, fiber_abn_by_type = self._get_summary(
            "fiber", FiberflappingAnalyzer, FLAP_DETAILS, "Max - Min (dB)"
        )
        self._render_row("Fiber", "Flapping", fiber_details, fiber_status, fiber_abn, "Max - Min (dB)")


        #4 ===== Export PDF รวม =====
        st.markdown("### Export Report")
        all_abnormal = {
            "CPU": cpu_abn_by_type,   # ✅ CPU มาก่อน
            "FAN": fan_abn_by_type,
            "MSU": msu_abn_by_type,
            "Line": line_abn_by_type,
            "Client": client_abn_by_type,
   

        }
        pdf_bytes = generate_report(all_abnormal=all_abnormal)
        st.download_button(
            label="Download Report (All Sections)",
            data=pdf_bytes,
            file_name="Network_Inspection_Report.pdf",
            mime="application/pdf",
        )

    def _render_row(self, type_name, task_name, details, status, df_abn, value_col: str, df_abn_by_type=None):
        """วาด summary row + toggle abnormal"""
        col1, col2, col3, col4, col5 = st.columns([1, 1, 3, 1, 1])
        col1.write(type_name)
        col2.write(task_name)
        col3.markdown(details.replace("\n", "<br>"), unsafe_allow_html=True)

        # Result cell
        if status == "Abnormal":
            col4.markdown(
                "<div style='background-color:#FFECEC; color:#B00020; font-weight:bold; "
                "text-align:center; padding:4px; border-radius:4px;'>Abnormal</div>",
                unsafe_allow_html=True
            )
        elif status == "Normal":
            col4.markdown(
                "<div style='background-color:#E6FFEC; color:#0F7B3E; font-weight:bold; "
                "text-align:center; padding:4px; border-radius:4px;'>Normal</div>",
                unsafe_allow_html=True
            )
        else:
            col4.write(status)

        # ✅ ใช้ key แยกสำหรับ state กับปุ่ม
        key_state = f"{task_name}_show_table"
        key_button = f"{task_name}_toggle_btn"

        if key_state not in st.session_state:
            st.session_state[key_state] = False

        if col5.button("View", key=key_button):
            st.session_state[key_state] = not st.session_state[key_state]

        # Drilldown abnormal table
        if st.session_state[key_state]:
            if status == "Abnormal" and df_abn is not None:
                st.markdown(f"#### Abnormal {task_name} Table")


         

                # ===================== CPU =====================
                if task_name == "CPU board":
                    cols_to_show = ["Site Name", "ME", "Measure Object",
                                    "Maximum threshold", "Minimum threshold", "CPU utilization ratio"]
                    df_abn = df_abn[cols_to_show].copy()

                    numeric_cols = [c for c in df_abn.columns if c not in ["Site Name", "ME", "Measure Object"]]
                    for c in numeric_cols:
                        df_abn[c] = pd.to_numeric(df_abn[c], errors="coerce")

                    styled = (
                        df_abn.style
                        .format({c: "{:.2f}" for c in numeric_cols}, na_rep="-")
                        .applymap(lambda v: "background-color:#ff9999; color:black"
                                if isinstance(v, (int, float)) and pd.notna(v) and v > 0 else "",
                                subset=["CPU utilization ratio"])
                    )
                    st.dataframe(styled, use_container_width=True)

                # ===================== FAN =====================
                elif task_name == "FAN board":
                    cols_to_show = ["Site Name", "ME", "Measure Object",
                                    "Maximum threshold", "Minimum threshold", "Value of Fan Rotate Speed(Rps)"]
                    df_abn = df_abn[cols_to_show].copy()

                    numeric_cols = [c for c in df_abn.columns if c not in ["Site Name", "ME", "Measure Object"]]
                    for c in numeric_cols:
                        df_abn[c] = pd.to_numeric(df_abn[c], errors="coerce")

                    styled = (
                        df_abn.style
                        .format({c: "{:.2f}" for c in numeric_cols}, na_rep="-")
                        .applymap(lambda v: "background-color:#ff9999; color:black"
                                if isinstance(v, (int, float)) and pd.notna(v) and v > 0 else "",
                                subset=["Value of Fan Rotate Speed(Rps)"])
                    )
                    st.dataframe(styled, use_container_width=True)

                # ===================== MSU =====================
                elif task_name == "MSU board":
                    cols_to_show = ["Site Name", "ME", "Measure Object",
                                    "Maximum threshold", "Laser Bias Current(mA)"]
                    df_abn = df_abn[cols_to_show].copy()

                    numeric_cols = [c for c in df_abn.columns if c not in ["Site Name", "ME", "Measure Object"]]
                    for c in numeric_cols:
                        df_abn[c] = pd.to_numeric(df_abn[c], errors="coerce")

                    styled = (
                        df_abn.style
                        .format({c: "{:.2f}" for c in numeric_cols}, na_rep="-")
                        .applymap(lambda v: "background-color:#ff9999; color:black"
                                if isinstance(v, (int, float)) and pd.notna(v) and v > 0 else "",
                                subset=["Laser Bias Current(mA)"])
                    )
                    st.dataframe(styled, use_container_width=True)

                # ===================== LINE =====================
                elif task_name == "Line board":
                    cols_to_show = [
                        "Site Name", "ME", "Call ID", "Measure Object",
                        "Threshold", "Instant BER After FEC",
                        "Maximum threshold(out)", "Minimum threshold(out)", "Output Optical Power (dBm)",
                        "Maximum threshold(in)", "Minimum threshold(in)", "Input Optical Power(dBm)",
                        "Route"
                    ]
                    df_abn = df_abn[[c for c in cols_to_show if c in df_abn.columns]].copy()

                    numeric_cols = [c for c in df_abn.columns if c not in ["Site Name", "ME", "Measure Object", "Call ID", "Route"]]
                    for c in numeric_cols:
                        df_abn[c] = pd.to_numeric(df_abn[c], errors="coerce")

                    def highlight_line_row(row):
                        styles = [""] * len(row)
                        col_map = {c: i for i, c in enumerate(df_abn.columns)}

                        try:
                            ber, thr = row["Instant BER After FEC"], row["Threshold"]
                            if pd.notna(ber) and pd.notna(thr) and ber > thr:
                                styles[col_map["Instant BER After FEC"]] = "background-color:#ff9999; color:black"
                        except:
                            pass

                        try:
                            v, lo, hi = row["Input Optical Power(dBm)"], row["Minimum threshold(in)"], row["Maximum threshold(in)"]
                            if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                                styles[col_map["Input Optical Power(dBm)"]] = "background-color:#ff9999; color:black"
                        except:
                            pass

                        try:
                            v, lo, hi = row["Output Optical Power (dBm)"], row["Minimum threshold(out)"], row["Maximum threshold(out)"]
                            if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                                styles[col_map["Output Optical Power (dBm)"]] = "background-color:#ff9999; color:black"
                        except:
                            pass

                        return styles

                    styled = (
                        df_abn.style
                        .apply(highlight_line_row, axis=1)
                        .format({
                            "Threshold": "{:.2E}",
                            "Instant BER After FEC": "{:.2E}",
                        }, na_rep="-")
                    )
                    st.dataframe(styled, use_container_width=True)

                # ===================== CLIENT =====================
                elif task_name == "Client board":
                    cols_to_show = [
                        "Site Name", "ME", "Measure Object",
                        "Maximum threshold(out)", "Minimum threshold(out)", "Output Optical Power (dBm)",
                        "Maximum threshold(in)", "Minimum threshold(in)", "Input Optical Power(dBm)"
                    ]
                    df_abn = df_abn[[c for c in cols_to_show if c in df_abn.columns]].copy()

                    def highlight_client_row(row):
                        styles = [""] * len(row)
                        col_map = {c: i for i, c in enumerate(df_abn.columns)}

                        try:
                            v, lo, hi = row["Output Optical Power (dBm)"], row["Minimum threshold(out)"], row["Maximum threshold(out)"]
                            if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                                styles[col_map["Output Optical Power (dBm)"]] = "background-color:#ff9999; color:black"
                        except:
                            pass

                        try:
                            v, lo, hi = row["Input Optical Power(dBm)"], row["Minimum threshold(in)"], row["Maximum threshold(in)"]
                            if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                                styles[col_map["Input Optical Power(dBm)"]] = "background-color:#ff9999; color:black"
                        except:
                            pass

                        return styles

                    styled = df_abn.style.apply(highlight_client_row, axis=1)
                    st.dataframe(styled, use_container_width=True)

            elif status == "Normal":
                st.info(f"✅ All {task_name} values are within normal range.")
            else:
                st.warning(f"⚠️ No {task_name} data available.")
