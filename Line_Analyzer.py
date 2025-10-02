# line_analyzer.py
import re
import pandas as pd
import streamlit as st
from utils.filters import cascading_filter
import plotly.express as px
import plotly.graph_objects as go

class Line_Analyzer:
    """
    ย้าย logic เดิมมารวมในคลาสเดียว:
      - อ่าน/จัดคอลัมน์
      - ตรวจ Required cols
      - Merge กับ Reference + ใส่ Preset จาก pmap
      - เรียงตาม order
      - Cascading filter
      - ไฮไลต์สี และสรุปสถานะ + Visuals

    NOTE:
    - พึ่งพา cascading_filter(ns=...) ที่มีอยู่ในโปรเจกต์เดิม
    - คงชื่อคอลัมน์และเงื่อนไขทั้งหมดให้เหมือนของเดิม
    """

    # ---------- พาร์เซพรีเซ็ตจาก WASON Log ----------
    @staticmethod
    def get_preset_map(log_text: str) -> dict:
        lines = log_text.splitlines()
        ipmap = {
            "30.10.90.6": "HYI-4",
            "30.10.10.6": "Jasmine",
            "30.10.30.6": "Phu Nga",
            "30.10.50.6": "SNI-POI",
            "30.10.70.6": "NKS",
            "30.10.110.6": "PKT",
        }
        pmap = {}
        i = 0
        while i < len(lines):
            line = lines[i]
            m = re.search(r"\[CALL\s+\d+\]\s+\[([\d.]+)\s+[\d.]+\s+(\d+)\]", line)
            if m:
                ip = m.group(1).strip()
                cid = m.group(2).strip().lstrip("0")
                site = ipmap.get(ip, "Unknown")
                key = f"{cid} ({site})"
                j = i + 1
                while j < len(lines):
                    if "[CALL" in lines[j]:
                        break
                    if "[PreRout]:" in lines[j]:
                        k = j + 1
                        while k < len(lines):
                            if "[CALL" in lines[k]:
                                break
                            m2 = re.search(r"--(\d+)--WORK--\(USED\)--\(SUCCESS\)", lines[k])
                            if m2:
                                preset = m2.group(1).strip()
                                pmap[cid] = preset
                                pmap.setdefault(key, preset)
                                break
                            k += 1
                        break
                    j += 1
            i += 1
        return pmap

    def __init__(self, df_line: pd.DataFrame, df_ref: pd.DataFrame, pmap: dict | None = None, ns: str = "line"):
        self.df_line = df_line
        self.df_ref  = df_ref
        self.pmap    = pmap or {}
        self.ns      = ns  # namespace ใช้ร่วมกับ cascading_filter

        # ชื่อคอลัมน์ตามเดิม
        self.col_in       = "Input Optical Power(dBm)"
        self.col_out      = "Output Optical Power (dBm)"
        self.col_min_in   = "Minimum threshold(in)"
        self.col_max_in   = "Maximum threshold(in)"
        self.col_min_out  = "Minimum threshold(out)"
        self.col_max_out  = "Maximum threshold(out)"


        # ---------- NEW: containers for Summary ----------
        self.df_abnormal = pd.DataFrame()
        self.df_abnormal_by_type = {}

    # ---------- Utilities ----------
    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.astype(str)
            .str.replace(r'\s+', ' ', regex=True)
            .str.replace('\u00a0', ' ')
            .str.strip()
        )
        return df

    def _check_required(self) -> None:
        required_cols = {
            "ME", "Measure Object", "Instant BER After FEC",
            self.col_in, self.col_out
        }
        missing = required_cols - set(self.df_line.columns)
        if missing:
            raise ValueError(f"Line cards file must contain columns: {', '.join(sorted(required_cols))}")

    def _merge_with_ref(self) -> pd.DataFrame:
        # เพิ่มลำดับ (ไว้เรียงภายหลัง)
        self.df_ref["order"] = range(len(self.df_ref))

        # สร้าง key แม็พ
        self.df_line["Mapping Format"] = (
            self.df_line["ME"].astype(str).str.strip()
            + self.df_line["Measure Object"].astype(str).str.strip()
        )
        self.df_ref["Mapping"] = self.df_ref["Mapping"].astype(str).str.strip()

        # เลือกคอลัมน์จาก ref ที่ใช้จริง
        cols_ref = [
            "Site Name", "Mapping", "Call ID", "Threshold",
            self.col_max_out, self.col_min_out, self.col_max_in, self.col_min_in,
            "Route", "order"
        ]
        df_merged = pd.merge(
            self.df_line,
            self.df_ref[cols_ref],
            left_on="Mapping Format",
            right_on="Mapping",
            how="inner"
        )
        return df_merged

    def _apply_preset_route(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Call ID"] = df["Call ID"].astype(str).str.strip().str.lstrip("0")
        df["Route"]   = df.apply(
            lambda r: f"Preset {self.pmap[r['Call ID']]}" if r["Call ID"] in self.pmap else r["Route"],
            axis=1
        )
        return df

    @staticmethod
    def _row_has_issue(r: pd.Series,
                       col_ber: str,
                       col_out: str, col_max_out: str, col_min_out: str,
                       col_in: str,  col_max_in: str,  col_min_in: str) -> bool:
        def _num(x):
            try:
                return float(x)
            except Exception:
                return float("nan")

        ber = _num(r.get(col_ber))
        vout, hi_out, lo_out = _num(r.get(col_out)), _num(r.get(col_max_out)), _num(r.get(col_min_out))
        vin,  hi_in,  lo_in  = _num(r.get(col_in)),  _num(r.get(col_max_in)),  _num(r.get(col_min_in))

        return (
            (pd.notna(ber) and ber > 0) or
            (pd.notna(vout) and pd.notna(hi_out) and pd.notna(lo_out) and (vout > hi_out or vout < lo_out)) or
            (pd.notna(vin)  and pd.notna(hi_in)  and pd.notna(lo_in)  and (vin  > hi_in  or vin  < lo_in))
        )

    def _style_dataframe(self, df_view: pd.DataFrame) -> pd.io.formats.style.Styler:
        col_ber = "Instant BER After FEC"

        # ✅ บังคับคอลัมน์ตัวเลขทั้งหมดให้เป็น float (กัน error format 'E')
        num_cols = [col_ber, "Threshold", self.col_out, self.col_in,
                    self.col_max_out, self.col_min_out, self.col_max_in, self.col_min_in]
        for c in num_cols:
            if c in df_view.columns:
                df_view[c] = pd.to_numeric(df_view[c], errors="coerce")

        def _has_issue_row(r):
            return self._row_has_issue(
                r,
                col_ber,
                self.col_out, self.col_max_out, self.col_min_out,
                self.col_in,  self.col_max_in,  self.col_min_in
            )

        styled = (
            df_view.style
            # 🌑 ไฮไลต์ gray ถ้ามีปัญหา
            .apply(lambda r: [
                'background-color:#e6e6e6; color:black' if _has_issue_row(r) else ''
                for _ in r
            ], axis=1)

            # 🔴 ไฮไลต์ BER abnormal
            .apply(lambda _: [
                'background-color:#ff4d4d; color:white'
                if (pd.notna(v) and v > 0) else ''
                for v in df_view[col_ber]
            ], subset=[col_ber] if col_ber in df_view.columns else [])

            # 🔴 ไฮไลต์ Output abnormal
            .apply(lambda _: [
                'background-color:#ff4d4d; color:white'
                if (pd.notna(v) and pd.notna(hi) and pd.notna(lo) and (v > hi or v < lo)) else ''
                for v, hi, lo in zip(
                    df_view.get(self.col_out, []),
                    df_view.get(self.col_max_out, []),
                    df_view.get(self.col_min_out, []),
                )
            ], subset=[self.col_out] if self.col_out in df_view.columns else [])

            # 🔴 ไฮไลต์ Input abnormal
            .apply(lambda _: [
                'background-color:#ff4d4d; color:white'
                if (pd.notna(v) and pd.notna(hi) and pd.notna(lo) and (v > hi or v < lo)) else ''
                for v, hi, lo in zip(
                    df_view.get(self.col_in, []),
                    df_view.get(self.col_max_in, []),
                    df_view.get(self.col_min_in, []),
                )
            ], subset=[self.col_in] if self.col_in in df_view.columns else [])

            # 🔵 ไฮไลต์ Route ที่เป็น Preset
            .apply(lambda _: [
                'background-color:lightblue; color:black'
                if str(x).startswith("Preset") else ''
                for x in df_view.get("Route", [])
            ], subset=["Route"] if "Route" in df_view.columns else [])

            # ✅ กำหนดรูปแบบการแสดงผล
            .format({
                self.col_out: "{:.4f}", 
                self.col_in: "{:.4f}",
                self.col_max_out: "{:.4f}", 
                self.col_min_out: "{:.4f}",
                self.col_max_in: "{:.4f}",  
                self.col_min_in: "{:.4f}",
                "Instant BER After FEC": "{:.2E}",   # 👈 ใช้ .2E ปลอดภัยชัวร์
                "Threshold": "{:.2E}",               # 👈
            })
        )
        return styled



    # ---------- NEW: รวมหลายแถวให้เหลือ 1 เส้น ----------
    def _collapse_by_line(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        รวมหลายแถวที่เป็นเส้นเดียวกัน (Site+ME+Call ID) ให้เหลือ 1 แถวตรรกะ
        - BER/Threshold: ดึงค่าจากแถว BER ถ้ามี
        - Power: รวมแบบ conservative range (min ใช้ค่ามากสุดของ mins, max ใช้ค่าน้อยสุดของ maxes)
        - Route: ถ้ามี 'Preset ...' ในกลุ่ม ให้เลือกอันนั้น มิฉะนั้นใช้ค่าแรก
        """
        key_cols = ["Site Name", "ME", "Call ID"]
        if not set(key_cols).issubset(df.columns):
            return df.copy()

        def _num(s):
            return pd.to_numeric(s, errors="coerce")

        rows = []
        for (site, me, cid), g in df.groupby(key_cols, dropna=False):
            g = g.copy()
            routes = g.get("Route", pd.Series([], dtype=object)).astype(str).tolist()
            route = next((r for r in routes if r.startswith("Preset")), routes[0] if routes else None)

            power_cols = [self.col_in, self.col_out, self.col_min_in, self.col_max_in, self.col_min_out, self.col_max_out]
            has_power = g[power_cols].notna().any(axis=1) if set(power_cols).issubset(g.columns) else pd.Series(False, index=g.index)
            mo = (g.loc[has_power, "Measure Object"].iloc[0]
                  if "Measure Object" in g.columns and has_power.any()
                  else (g["Measure Object"].iloc[0] if "Measure Object" in g.columns and len(g) else None))

            ber = _num(g.get("Instant BER After FEC", pd.Series(dtype=float))).dropna()
            thr = _num(g.get("Threshold", pd.Series(dtype=float))).dropna()
            ber_val = ber.iloc[0] if len(ber) else float("nan")
            thr_val = thr.iloc[0] if len(thr) else float("nan")

            vin_vals    = _num(g.get(self.col_in, pd.Series(dtype=float))).dropna()
            vout_vals   = _num(g.get(self.col_out, pd.Series(dtype=float))).dropna()
            min_in_vals = _num(g.get(self.col_min_in, pd.Series(dtype=float))).dropna()
            max_in_vals = _num(g.get(self.col_max_in, pd.Series(dtype=float))).dropna()
            min_out_vals= _num(g.get(self.col_min_out, pd.Series(dtype=float))).dropna()
            max_out_vals= _num(g.get(self.col_max_out, pd.Series(dtype=float))).dropna()

            vin   = vin_vals.iloc[0] if len(vin_vals) else float("nan")
            vout  = vout_vals.iloc[0] if len(vout_vals) else float("nan")
            min_in  = min_in_vals.max()  if len(min_in_vals) else float("nan")  # narrowest lower bound
            max_in  = max_in_vals.min()  if len(max_in_vals) else float("nan")  # narrowest upper bound
            min_out = min_out_vals.max() if len(min_out_vals) else float("nan")
            max_out = max_out_vals.min() if len(max_out_vals) else float("nan")

            rows.append({
                "Site Name": site, "ME": me, "Call ID": str(cid),
                "Measure Object": mo, "Route": route,
                "Threshold": thr_val, "Instant BER After FEC": ber_val,
                self.col_max_out: max_out, self.col_min_out: min_out, self.col_out: vout,
                self.col_max_in:  max_in,  self.col_min_in:  min_in,  self.col_in:  vin,
            })

        return pd.DataFrame(rows)

    # ---------- MAIN PIPELINE ----------
    def process(self) -> None:
        # 1) Normalize columns
        self.df_line = self._normalize_columns(self.df_line)
        self.df_ref  = self._normalize_columns(self.df_ref)

        # 2) ตรวจ required
        self._check_required()

        # 3) Merge กับ reference
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            st.warning("No matching mapping found between Line file and reference")
            return

        # 4) เลือกคอลัมน์ที่จะแสดง (ตามของเดิม)
        self.main_cols = [
            "Site Name", "ME", "Call ID", "Measure Object", "Threshold", "Instant BER After FEC",
            self.col_max_out, self.col_min_out, self.col_out,
            self.col_max_in, self.col_min_in, self.col_in, "Route", "order"
        ]
        df_result = df_merged[self.main_cols]

        # 5) ใส่ Preset จาก pmap
        df_result = self._apply_preset_route(df_result)

        # 6) เรียงตาม order แล้วทิ้งคอลัมน์ช่วย
        df_result = df_result.sort_values("order").drop(columns=["order"]).reset_index(drop=True)

        # 7) FILTER แบบ cascading
        df_filtered, _sel = cascading_filter(
            df_result,
            cols=["Site Name", "ME", "Measure Object", "Call ID", "Route"],
            ns=self.ns,
            clear_text="Clear Line Filters",
        )
        st.caption(f"Line Performance (showing {len(df_filtered)}/{len(df_result)} rows)")


        # 8) สไตล์/ไฮไลต์ (ตารางดิบเพื่อการตรวจละเอียด)
        styled = self._style_dataframe(df_filtered.copy())

        # 9) แสดงผลตาราง
        st.markdown("### Line Performance")
        st.dataframe(styled, use_container_width=True)

        # 10) รวมระดับ "เส้น" เพื่อใช้คำนวณ/กราฟให้ถูกต้อง
        df_lines = self._collapse_by_line(df_filtered.copy())

        # 11) สรุปสถานะหัวเรื่องจากระดับ "เส้น"
        def _line_fail(row: pd.Series) -> bool:
            ber = pd.to_numeric(pd.Series([row.get("Instant BER After FEC")]))[0]
            thr = pd.to_numeric(pd.Series([row.get("Threshold")]))[0]
            vin = pd.to_numeric(pd.Series([row.get(self.col_in)]))[0]
            vout= pd.to_numeric(pd.Series([row.get(self.col_out)]))[0]
            min_in  = pd.to_numeric(pd.Series([row.get(self.col_min_in)]))[0]
            max_in  = pd.to_numeric(pd.Series([row.get(self.col_max_in)]))[0]
            min_out = pd.to_numeric(pd.Series([row.get(self.col_min_out)]))[0]
            max_out = pd.to_numeric(pd.Series([row.get(self.col_max_out)]))[0]

            fail_ber  = (pd.notna(thr) and pd.notna(ber) and ber > thr) or (pd.isna(thr) and pd.notna(ber) and ber != 0)
            fail_in   = (pd.notna(vin) and pd.notna(min_in) and pd.notna(max_in) and not (min_in <= vin <= max_in))
            fail_out  = (pd.notna(vout) and pd.notna(min_out) and pd.notna(max_out) and not (min_out <= vout <= max_out))
            return bool(fail_ber or fail_in or fail_out)

        failed_lines = df_lines.apply(_line_fail, axis=1)
        st.markdown(
            "<div style='text-align:center; font-size:32px; font-weight:bold; color:{};'>Line Performance {}</div>".format(
                "red" if failed_lines.any() else "green",
                "Warning" if failed_lines.any() else "Normal"
            ),
            unsafe_allow_html=True
        )

        # ---------- VISUALS ----------
        self._render_summary_kpi(df_lines)                 # Summary KPI
        self._render_ber_donut(df_lines)                   # BER Donut
        self._render_line_charts(df_lines)                 # Line Chart
        self._render_preset_kpi_and_drilldown(df_lines)    # Preset KPI + Drill-down

        # ---------- VISUALS (KPI, Donut, Line Chart, Preset) ----------
    def _render_summary_kpi(self, df_view: pd.DataFrame) -> None:
        """Summary KPI: BER / Input / Output / Preset Usage (ระดับเส้น)"""
        st.markdown("### Summary KPI")

        # --- BER ---
        ber = pd.to_numeric(df_view["Instant BER After FEC"].astype(str), errors="coerce").astype(float)
        thr = pd.to_numeric(df_view["Threshold"].astype(str), errors="coerce").astype(float)

        ok = (
            ((thr > 0) & (ber <= thr)) |
            ((thr == 0) & (ber == 0))
        )
        ok_ber_cnt = int(ok.sum())
        total_ber = len(df_view)
        fail_ber_cnt = total_ber - ok_ber_cnt

        # --- Input ---
        vin = pd.to_numeric(df_view.get(self.col_in), errors="coerce")
        min_in = pd.to_numeric(df_view.get(self.col_min_in), errors="coerce")
        max_in = pd.to_numeric(df_view.get(self.col_max_in), errors="coerce")

        total_in = len(df_view)
        ok_in = (vin >= min_in) & (vin <= max_in)
        ok_in_cnt = int(ok_in.sum())
        fail_in_cnt = total_in - ok_in_cnt

        # --- Output ---
        vout = pd.to_numeric(df_view.get(self.col_out), errors="coerce")
        min_out = pd.to_numeric(df_view.get(self.col_min_out), errors="coerce")
        max_out = pd.to_numeric(df_view.get(self.col_max_out), errors="coerce")

        total_out = len(df_view)
        ok_out = (vout >= min_out) & (vout <= max_out)
        ok_out_cnt = int(ok_out.sum())
        fail_out_cnt = total_out - ok_out_cnt

        # --- Preset Usage ---
        if "Route" in df_view.columns:
            mask_preset = df_view["Route"].astype(str).str.startswith("Preset")
        else:
            mask_preset = pd.Series(False, index=df_view.index)

        preset_rows = df_view[mask_preset]
        preset_used = int(mask_preset.sum())
        preset_fail = 0

        if not preset_rows.empty:
            p_ber = pd.to_numeric(preset_rows.get("Instant BER After FEC"), errors="coerce")
            p_thr = pd.to_numeric(preset_rows.get("Threshold"), errors="coerce")
            p_vin = pd.to_numeric(preset_rows.get(self.col_in), errors="coerce")
            p_min_in = pd.to_numeric(preset_rows.get(self.col_min_in), errors="coerce")
            p_max_in = pd.to_numeric(preset_rows.get(self.col_max_in), errors="coerce")
            p_vout = pd.to_numeric(preset_rows.get(self.col_out), errors="coerce")
            p_min_out = pd.to_numeric(preset_rows.get(self.col_min_out), errors="coerce")
            p_max_out = pd.to_numeric(preset_rows.get(self.col_max_out), errors="coerce")

            p_fail_ber = (p_ber > p_thr)
            p_fail_in = (p_vin < p_min_in) | (p_vin > p_max_in)
            p_fail_out = (p_vout < p_min_out) | (p_vout > p_max_out)

            preset_fail = int((p_fail_ber | p_fail_in | p_fail_out).sum())

        # ---------- Show KPI ----------
        cols = st.columns(4)
        cols[0].metric("BER OK", f"{ok_ber_cnt}", f"{fail_ber_cnt} Fail")
        cols[1].metric("Input OK", f"{ok_in_cnt}", f"{fail_in_cnt} Fail")
        cols[2].metric("Output OK", f"{ok_out_cnt}", f"{fail_out_cnt} Fail")
        cols[3].metric("Preset Usage", f"{preset_used}", f"{preset_fail} Fail")


    def _render_ber_donut(self, df_view: pd.DataFrame) -> None:
        """แสดง BER Donut (OK vs Fail) — นับระดับเส้นที่วัด BER จริงเท่านั้น"""
        if "Instant BER After FEC" not in df_view.columns:
            return

        ber = pd.to_numeric(df_view["Instant BER After FEC"].astype(str), errors="coerce")
        thr = pd.to_numeric(df_view["Threshold"].astype(str), errors="coerce")
        measured = ber.notna() | thr.notna()
        ok = (((thr.notna()) & (ber.notna()) & (ber <= thr)) |
            ((thr.isna()) & (ber.notna()) & (ber == 0))) & measured
        ok_cnt = int(ok.sum())
        total = int(measured.sum())
        fail_cnt = max(0, total - ok_cnt)

        # ----- Donut Chart -----
        fig = px.pie(
            names=["OK", "Fail"],
            values=[ok_cnt, fail_cnt],
            color=["OK", "Fail"],
            color_discrete_map={"OK": "green", "Fail": "red"},
            hole=0.45,
            title="BER Status (OK vs Fail)"
        )
        # 🔧 เปลี่ยนจาก percent → value
        fig.update_traces(textinfo="label+value")

        st.plotly_chart(fig, use_container_width=True)

        
        # ----- Fail Details -----
        fail_rows = df_view[
            (pd.to_numeric(df_view["Instant BER After FEC"], errors="coerce") >
            pd.to_numeric(df_view["Threshold"], errors="coerce"))
        ][[
            "Site Name", "ME", "Call ID", "Measure Object", "Threshold", "Instant BER After FEC"
        ]]

        # ---------- NEW: Save to state ----------

        
        if not fail_rows.empty:
            st.markdown("**Problem Call IDs (BER above threshold)**")

            def highlight_line_row(row):
                styles = [""] * len(row)
                col_map = {c: i for i, c in enumerate(fail_rows.columns)}

                # ✅ BER check
                try:
                    ber = float(row["Instant BER After FEC"])
                    thr = float(row["Threshold"])
                    if pd.notna(ber) and pd.notna(thr) and ber > thr:
                        styles[col_map["Instant BER After FEC"]] = "background-color:#ff4d4d; color:white"
                except:
                    pass

                # ✅ Input check
                if "Input Optical Power(dBm)" in row:
                    try:
                        v = float(row["Input Optical Power(dBm)"])
                        lo = float(row["Minimum threshold(in)"])
                        hi = float(row["Maximum threshold(in)"])
                        if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                            styles[col_map["Input Optical Power(dBm)"]] = "background-color:#ff4d4d; color:white"
                    except:
                        pass

                # ✅ Output check
                if "Output Optical Power (dBm)" in row:
                    try:
                        v = float(row["Output Optical Power (dBm)"])
                        lo = float(row["Minimum threshold(out)"])
                        hi = float(row["Maximum threshold(out)"])
                        if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                            styles[col_map["Output Optical Power (dBm)"]] = "background-color:#ff4d4d; color:white"
                    except:
                        pass

                return styles

           
            fail_rows = fail_rows.reset_index(drop=True)

            styled = (
                fail_rows.style
                .apply(highlight_line_row, axis=1)
                .format({
                    "Threshold": "{:.2E}",
                    "Instant BER After FEC": "{:.2E}"
                }, na_rep="-")
            )
            st.dataframe(styled, use_container_width=True)



    def _render_line_charts(self, df_view: pd.DataFrame) -> None:
        """Plot Line Chart สำหรับ Board LB2R และ L4S (ใช้แถวจริงจากตารางหลัก)"""
        st.markdown("### Line Board Performance (LB2R & L4S)")

        def _plot_board(df_board_raw: pd.DataFrame, board_name: str):
            if df_board_raw.empty:
                st.info(f"No {board_name} rows found.")
                return

            # ---------- ใช้เฉพาะแถวที่มีค่า I/O จริง (กันแถว BER-only ออก) ----------
            vin_raw  = pd.to_numeric(df_board_raw.get(self.col_in),  errors="coerce")
            vout_raw = pd.to_numeric(df_board_raw.get(self.col_out), errors="coerce")
            mask_io  = vin_raw.notna() | vout_raw.notna()

            df_board = df_board_raw.loc[mask_io].copy()
            if df_board.empty:
                st.info(f"No {board_name} I/O rows found.")
                return

            # ---------- เตรียมข้อมูลสำหรับกราฟ ----------
            x_index     = list(range(len(df_board)))
            x_vals      = df_board["Measure Object"]
            site_labels = df_board["Site Name"]

            vin     = pd.to_numeric(df_board.get(self.col_in),        errors="coerce")
            vout    = pd.to_numeric(df_board.get(self.col_out),       errors="coerce")
            min_in  = pd.to_numeric(df_board.get(self.col_min_in),    errors="coerce")
            max_in  = pd.to_numeric(df_board.get(self.col_max_in),    errors="coerce")
            min_out = pd.to_numeric(df_board.get(self.col_min_out),   errors="coerce")
            max_out = pd.to_numeric(df_board.get(self.col_max_out),   errors="coerce")

            # ---------- สร้างกราฟ ----------
            fig = go.Figure()
            # Threshold bands Input
            fig.add_traces([
                go.Scatter(x=x_index, y=min_in, mode="lines", line=dict(color="orange", dash="dot"), name="Input Min"),
                go.Scatter(x=x_index, y=max_in, mode="lines", line=dict(color="orange", dash="dot"),
                        name="Input Max", fill="tonexty", fillcolor="rgba(255,165,0,0.1)")
            ])
            # Threshold bands Output
            fig.add_traces([
                go.Scatter(x=x_index, y=min_out, mode="lines", line=dict(color="blue", dash="dot"), name="Output Min"),
                go.Scatter(x=x_index, y=max_out, mode="lines", line=dict(color="blue", dash="dot"),
                        name="Output Max", fill="tonexty", fillcolor="rgba(0,0,255,0.1)")
            ])
            # Input Power
            # ---------- เตรียมสีจุด Input ----------
            vin_colors = [
                "red" if (pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi)) else "orange"
                for v, lo, hi in zip(vin, min_in, max_in)
            ]

            # ---------- เตรียมสีจุด Output ----------
            vout_colors = [
                "red" if (pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi)) else "blue"
                for v, lo, hi in zip(vout, min_out, max_out)
            ]

            # ---------- Input Power ----------
            fig.add_trace(go.Scatter(
                x=x_index, y=vin, mode="lines+markers",
                marker=dict(color=vin_colors, size=8, symbol="circle"),
                line=dict(color="orange"), name="Input Power",
                text=x_vals, customdata=site_labels,
                hovertemplate="Site=%{customdata}<br>Port=%{text}<br>Input=%{y:.4f} dBm<extra></extra>"
            ))

            # ---------- Output Power ----------
            fig.add_trace(go.Scatter(
                x=x_index, y=vout, mode="lines+markers",
                marker=dict(color=vout_colors, size=8, symbol="square"),
                line=dict(color="blue"), name="Output Power",
                text=x_vals, customdata=site_labels,
                hovertemplate="Site=%{customdata}<br>Port=%{text}<br>Output=%{y:.4f} dBm<extra></extra>"
            ))

            # ---------- Layout ----------
            fig.update_layout(
                title=f"Board{board_name} ",
                yaxis_title="Optical Power (dBm)",
                xaxis=dict(
                    title="Site Name",
                    tickmode="array",
                    tickvals=x_index,
                    ticktext=site_labels,
                    tickangle=45,
                    automargin=True
                ),
                legend=dict(orientation="h", y=1.15),
                height=600,
                margin=dict(b=150)
            )

            st.plotly_chart(fig, use_container_width=True)


            # ---------- Problem Lines: ดึงบรรทัดจริงจาก df_board_raw ----------
            min_in   = pd.to_numeric(df_board_raw.get(self.col_min_in),  errors="coerce")
            max_in   = pd.to_numeric(df_board_raw.get(self.col_max_in),  errors="coerce")
            min_out  = pd.to_numeric(df_board_raw.get(self.col_min_out), errors="coerce")
            max_out  = pd.to_numeric(df_board_raw.get(self.col_max_out), errors="coerce")

            mask_problem = (
                (vin_raw.notna()  & min_in.notna()  & max_in.notna()  & ((vin_raw  < min_in)  | (vin_raw  > max_in))) |
                (vout_raw.notna() & min_out.notna() & max_out.notna() & ((vout_raw < min_out) | (vout_raw > max_out)))
            )

            df_problems = df_board_raw.loc[mask_problem] 
             # 👈 บรรทัดจริง 100%

            
       
            if not df_problems.empty:
                st.markdown(f"**⚠️ Problem Lines for {board_name}:**")

                def highlight_line_row(row):
                    styles = [""] * len(row)
                    col_map = {c: i for i, c in enumerate(df_problems.columns)}

                    # ✅ Input check
                    try:
                        v = float(row[self.col_in])
                        lo = float(row[self.col_min_in])
                        hi = float(row[self.col_max_in])
                        if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                            styles[col_map[self.col_in]] = "background-color:#ff4d4d; color:white"
                    except:
                        pass

                    # ✅ Output check
                    try:
                        v = float(row[self.col_out])
                        lo = float(row[self.col_min_out])
                        hi = float(row[self.col_max_out])
                        if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                            styles[col_map[self.col_out]] = "background-color:#ff4d4d; color:white"
                    except:
                        pass

                    return styles

                styled = (
                    df_problems.style
                    .apply(highlight_line_row, axis=1)
                    .format({
                        "Threshold": "{:.2E}",
                        "Instant BER After FEC": "{:.2E}"
                    }, na_rep="-")
                )
                st.dataframe(styled, use_container_width=True)


            else:
                st.success(f"All {board_name} lines are within threshold.")
                st.markdown("<br><br>", unsafe_allow_html=True)

         


        # ---------- แยกบอร์ดจากแถวจริง ----------
        df_lb2r_raw = df_view[df_view["Measure Object"].astype(str).str.contains("LB2R", na=False)]
        df_l4s_raw  = df_view[df_view["Measure Object"].astype(str).str.contains("L4S",  na=False)]

        _plot_board(df_lb2r_raw, "LB2R")

        # ---------- เพิ่ม Dropdown เลือกกราฟ L4S ----------
        options = {
            "L4S (All Sites)": df_l4s_raw,
            "L4S (HYI-4 Jastel_Z-E33, HYI-4 Jastel_Z-E34)": df_l4s_raw[df_l4s_raw["Site Name"].isin([
                "HYI-4 Jastel_Z-E33", "HYI-4 Jastel_Z-E34"
            ])],
            "L4S (Jasmine_Z-E33, Jasmine_Z-E34)": df_l4s_raw[df_l4s_raw["Site Name"].isin([
                "Jasmine_Z-E33", "Jasmine_Z-E34"
            ])],
            "L4S (Phu Nga_Z-E33, Phu Nga_Z-E34, Phuket_Z-E33, Phuket_Z-E34, SNI-POI_Z-E33, SNI-POI_Z-E34)": 
                df_l4s_raw[df_l4s_raw["Site Name"].isin([
                    "Phu Nga_Z-E33", "Phu Nga_Z-E34",
                    "Phuket_Z-E33", "Phuket_Z-E34",
                    "SNI-POI_Z-E33", "SNI-POI_Z-E34"
                ])],
        }


        choice = st.selectbox("Select L4S Site(s) to Display", list(options.keys()))
        df_selected = options[choice].copy()

        # ✅ กำหนดคอลัมน์ตามตารางหลัก
        main_cols = [
            "Site Name", "ME", "Call ID", "Measure Object", "Threshold", "Instant BER After FEC",
            self.col_max_out, self.col_min_out, self.col_out,
            self.col_max_in, self.col_min_in, self.col_in, "Route"
        ]

        # ✅ เรียงคอลัมน์ subset ให้ตรงกับตารางหลัก
        df_selected = df_selected[[c for c in main_cols if c in df_selected.columns]]

        if not df_selected.empty:
            _plot_board(df_selected, choice)
        else:
            st.warning("ไม่มีข้อมูลในกลุ่มนี้")




    def _render_preset_kpi_and_drilldown(self, df_view: pd.DataFrame) -> None:
        """Preset KPI + Drill-down (ระดับเส้น)"""
        st.markdown("<br><br><br><br>", unsafe_allow_html=True)
        st.markdown("### Preset KPI + Drill-down")
        df_preset = df_view[df_view["Route"].astype(str).str.startswith("Preset")].copy()
        if df_preset.empty:
            st.info("No Preset routes found.")
            return

        df_preset["PresetNo"] = df_preset["Route"].astype(str).str.extract(r"Preset\s*(\d+)")
        df_preset["Call ID"] = df_preset["Call ID"].astype(str).str.strip()
        self.df_preset_kpi = df_preset.copy()


        groups = df_preset.groupby("PresetNo")

        cols = st.columns(min(3, len(groups)))
        for idx, (preset, sub) in enumerate(groups):
            with cols[idx % len(cols)]:
                with st.expander(f"Preset {preset} — {len(sub)} lines"):
                    for _, r in sub.iterrows():
                        st.write(f"- **{r['Site Name']}** → {r['Route']} _(Call {r['Call ID']})_")

                    if st.button(f"Show details: Preset {preset}", key=f"btn_preset_{preset}"):
                        # ✅ ใช้ลำดับคอลัมน์จาก self.main_cols (ตัด order ออก)
                        # ✅ กำหนดคอลัมน์ที่จะโชว์เฉพาะ Preset Drilldown
                        preset_cols = ["Site Name", "ME", "Call ID", "Measure Object", "Route"]

                        df_show = sub[[c for c in preset_cols if c in sub.columns]]
                        st.dataframe(df_show.reset_index(drop=True), use_container_width=True)

    # ---------- NEW: PREPARE (Summary/PDF) ----------
    def prepare(self) -> None:
        """เตรียม abnormal ทั้งหมดสำหรับ Summary/PDF (ไม่ render UI)"""
        # 1) Normalize
        self.df_line = self._normalize_columns(self.df_line)
        self.df_ref  = self._normalize_columns(self.df_ref)

        # 2) Check required
        self._check_required()

        # 3) Merge
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            # reset containers
            self.df_abnormal = pd.DataFrame()
            self.df_abnormal_by_type = {}
            # save to session_state #4
            st.session_state["line_analyzer"] = self
            st.session_state["line_status"]   = "No data"
            st.session_state["line_abn_count"] = 0
            return

        # 4) Build result & apply preset
        df_result = df_merged[[
            "Site Name", "ME", "Call ID", "Measure Object", "Route",
            "Threshold", "Instant BER After FEC",
            self.col_max_out, self.col_min_out, self.col_out,
            self.col_max_in, self.col_min_in, self.col_in
        ]].reset_index(drop=True)
        df_result = self._apply_preset_route(df_result)

        # 5) Detect abnormal groups

        # 5.1 BER abnormal (Instant BER After FEC > Threshold)
        ber_val = pd.to_numeric(df_result["Instant BER After FEC"], errors="coerce")
        thr_val = pd.to_numeric(df_result["Threshold"], errors="coerce")
        mask_ber = (pd.notna(ber_val) & pd.notna(thr_val) & (ber_val > thr_val))
        df_ber = df_result.loc[mask_ber, ["Site Name", "ME", "Call ID", "Measure Object", "Threshold", "Instant BER After FEC"]].copy()

        # 5.2 LB2R abnormal (power out of range)
        df_lb2r = df_result[df_result["Measure Object"].astype(str).str.contains("LB2R", na=False)].copy()
        vin = pd.to_numeric(df_lb2r[self.col_in], errors="coerce")
        vout = pd.to_numeric(df_lb2r[self.col_out], errors="coerce")
        min_in = pd.to_numeric(df_lb2r[self.col_min_in], errors="coerce")
        max_in = pd.to_numeric(df_lb2r[self.col_max_in], errors="coerce")
        min_out = pd.to_numeric(df_lb2r[self.col_min_out], errors="coerce")
        max_out = pd.to_numeric(df_lb2r[self.col_max_out], errors="coerce")
        mask_lb2r = (
            (vin.notna() & min_in.notna() & max_in.notna() & ((vin < min_in) | (vin > max_in))) |
            (vout.notna() & min_out.notna() & max_out.notna() & ((vout < min_out) | (vout > max_out)))
        )
        df_lb2r = df_lb2r.loc[mask_lb2r, [
            "Site Name", "ME", "Call ID", "Measure Object", "Threshold", "Instant BER After FEC",
            self.col_max_out, self.col_min_out, self.col_out,
            self.col_max_in, self.col_min_in, self.col_in, "Route"
        ]].copy()

        # 5.3 L4S abnormal (power out of range)
        df_l4s = df_result[df_result["Measure Object"].astype(str).str.contains("L4S", na=False)].copy()
        vin = pd.to_numeric(df_l4s[self.col_in], errors="coerce")
        vout = pd.to_numeric(df_l4s[self.col_out], errors="coerce")
        min_in = pd.to_numeric(df_l4s[self.col_min_in], errors="coerce")
        max_in = pd.to_numeric(df_l4s[self.col_max_in], errors="coerce")
        min_out = pd.to_numeric(df_l4s[self.col_min_out], errors="coerce")
        max_out = pd.to_numeric(df_l4s[self.col_max_out], errors="coerce")
        mask_l4s = (
            (vin.notna() & min_in.notna() & max_in.notna() & ((vin < min_in) | (vin > max_in))) |
            (vout.notna() & min_out.notna() & max_out.notna() & ((vout < min_out) | (vout > max_out)))
        )
        df_l4s = df_l4s.loc[mask_l4s, [
            "Site Name", "ME", "Call ID", "Measure Object", "Threshold", "Instant BER After FEC",
            self.col_max_out, self.col_min_out, self.col_out,
            self.col_max_in, self.col_min_in, self.col_in, "Route"
        ]].copy()

        # 5.4 Preset abnormal (Route startswith 'Preset')
        df_preset = df_result[df_result["Route"].astype(str).str.startswith("Preset")].copy()
        df_preset = df_preset[["Site Name", "ME", "Call ID", "Measure Object", "Route"]].copy()

        # 6) Save results to properties 
        self.df_abnormal_by_type = {
            "BER": df_ber,
            "LB2R": df_lb2r,
            "L4S": df_l4s,
            "Preset": df_preset
        } #2
        self.df_abnormal = pd.concat(
            [df for df in self.df_abnormal_by_type.values() if not df.empty],
            ignore_index=True
        ) if any(not df.empty for df in self.df_abnormal_by_type.values()) else pd.DataFrame()

        # 7) Save to session_state 
        st.session_state["line_analyzer"] = self
        st.session_state["line_status"]   = "Abnormal" if not self.df_abnormal.empty else "Normal"
        st.session_state["line_abn_count"] = len(self.df_abnormal)
