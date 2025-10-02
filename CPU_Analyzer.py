# cpu_analyzer.py
import pandas as pd
import streamlit as st
from utils.filters import cascading_filter
from pandas.io.formats.style import Styler
import altair as alt


class CPU_Analyzer:
    """
    วิเคราะห์ CPU:
      - ตรวจคอลัมน์ที่ต้องมี
      - Merge กับ reference
      - ฟิลเตอร์แบบ cascading_filter
      - ไฮไลต์สี: เทาแถว, แดงค่าผิด threshold, ฟ้า Route ที่เป็น Preset
      - สรุปสถานะ Warning/Normal
      - แสดง Visualization: Bar Chart + Heatmap
    """

    def __init__(self, df_cpu: pd.DataFrame, df_ref: pd.DataFrame, ns: str = "cpu"):
        self.df_cpu = df_cpu
        self.df_ref = df_ref
        self.ns     = ns

        # column name mapping
        self.COL_ME   = "ME"
        self.COL_MOBJ = "Measure Object"
        self.COL_VAL  = "CPU utilization ratio"
        self.COL_MAX  = "Maximum threshold"
        self.COL_MIN  = "Minimum threshold"
        self.COL_SITE = "Site Name"

        # abnormal storage (เพิ่มเหมือน FAN)
        self.df_abnormal = pd.DataFrame()   # abnormal ทั้งหมด
        self.df_abnormal_by_type = {}       # abnormal แยกตาม BoardType (SNP(E), NCPM, NCPQ)

    # ---------- Utilities ----------
    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.replace("\u00a0", " ")
        )
        return df

    def _check_required(self) -> None:
        required_cols = {self.COL_ME, self.COL_MOBJ, self.COL_VAL}
        missing = required_cols - set(self.df_cpu.columns)
        if missing:
            raise ValueError(f"CPU file must contain columns: {', '.join(sorted(required_cols))}")

    def _check_required_ref(self) -> None:
        required_ref_cols = {"Mapping", self.COL_MAX, self.COL_MIN}
        missing = required_ref_cols - set(self.df_ref.columns)
        if missing:
            raise ValueError(f"Reference file must contain columns: {', '.join(sorted(required_ref_cols))}")

    def _merge_with_ref(self) -> pd.DataFrame:
        self.df_cpu["Mapping Format"] = (
            self.df_cpu[self.COL_ME].astype(str).str.strip()
            + self.df_cpu[self.COL_MOBJ].astype(str).str.strip()
        )
        self.df_ref["Mapping"] = self.df_ref["Mapping"].astype(str).str.strip()
        self.df_ref["order"]   = range(len(self.df_ref))

        ref_cols = ["Mapping", self.COL_MAX, self.COL_MIN, "order"]
        for extra in ["Site Name", "Call ID", "Route"]:
            if extra in self.df_ref.columns:
                ref_cols.append(extra)

        df_merged = pd.merge(
            self.df_cpu,
            self.df_ref[ref_cols],
            left_on="Mapping Format",
            right_on="Mapping",
            how="inner"
        )
        return df_merged

    @staticmethod
    def _row_has_issue(r, col_val, col_min, col_max) -> bool:
        v  = r.get(col_val)
        lo = r.get(col_min)
        hi = r.get(col_max)
        try:
            return (pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi))
        except:
            return False

    def _style_dataframe(self, df_view: pd.DataFrame) -> Styler:
        for c in [self.COL_VAL, self.COL_MAX, self.COL_MIN]:
            if c in df_view.columns:
                df_view[c] = pd.to_numeric(df_view[c], errors="coerce")

        # 🔹 ตรวจว่าเป็น ratio (0–1) หรือ % อยู่แล้ว
        max_val = df_view[self.COL_VAL].max()
        if pd.notna(max_val) and max_val <= 1:
            df_view[self.COL_VAL] = df_view[self.COL_VAL] * 100
        if "Maximum threshold" in df_view.columns and df_view[self.COL_MAX].max() <= 1:
            df_view[self.COL_MAX] = df_view[self.COL_MAX] * 100
        if "Minimum threshold" in df_view.columns and df_view[self.COL_MIN].max() <= 1:
            df_view[self.COL_MIN] = df_view[self.COL_MIN] * 100

        def gray_row(r):
            return [
                'background-color:#e6e6e6;color:black'
                if self._row_has_issue(r, self.COL_VAL, self.COL_MIN, self.COL_MAX) else ''
                for _ in r
            ]

        def red_value(_):
            return [
                'background-color:#ff4d4d;color:white'
                if (pd.notna(v) and pd.notna(hi) and pd.notna(lo) and (v > hi or v < lo))
                else ''
                for v, hi, lo in zip(
                    df_view.get(self.COL_VAL, pd.Series(index=df_view.index)),
                    df_view.get(self.COL_MAX, pd.Series(index=df_view.index)),
                    df_view.get(self.COL_MIN, pd.Series(index=df_view.index)),
                )
            ]

        def blue_route(_):
            return [
                'background-color:lightblue;color:black' if str(x).startswith("Preset") else ''
                for x in df_view["Route"]
            ] if "Route" in df_view.columns else []

        styled = (
            df_view.style
            .apply(gray_row, axis=1)
            .apply(red_value, subset=[self.COL_VAL] if self.COL_VAL in df_view.columns else [])
            .apply(blue_route, subset=["Route"] if "Route" in df_view.columns else [])
            .format({
                self.COL_VAL: "{:.2f}%",
                self.COL_MAX: "{:.2f}%",
                self.COL_MIN: "{:.2f}%",
            })
        )
        return styled

    # ---------- MAIN ----------
    def process(self) -> pd.DataFrame:
        # 1) Normalize
        self.df_cpu = self._normalize_columns(self.df_cpu)
        self.df_ref = self._normalize_columns(self.df_ref)

        # 2) Check
        self._check_required()
        self._check_required_ref()

        # 3) Merge
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            st.warning("No matching mapping found between CPU file and reference")
            return pd.DataFrame()

        # 4) Pick columns
        base_cols = [self.COL_ME, self.COL_MOBJ, self.COL_MAX, self.COL_MIN, self.COL_VAL, "order"]
        opt_cols  = [c for c in ["Site Name", "Call ID", "Route"] if c in df_merged.columns]
        show_cols = opt_cols + base_cols

        df_result = df_merged[show_cols].copy()
        df_result = df_result.sort_values("order").drop(columns=["order"]).reset_index(drop=True)

        # 5) Cascading filter
        df_filtered, _sel = cascading_filter(
            df_result,
            cols=["Site Name", self.COL_ME, self.COL_MOBJ],
            ns=self.ns,
            clear_text="Clear CPU Filters"
        )
        st.caption(f"CPU (showing {len(df_filtered)}/{len(df_result)} rows)")

        # 6) Overall status + abnormal เก็บเหมือน FAN
        val = pd.to_numeric(df_result[self.COL_VAL], errors="coerce")
        hi  = pd.to_numeric(df_result[self.COL_MAX], errors="coerce")
        lo  = pd.to_numeric(df_result[self.COL_MIN], errors="coerce")
        ab_mask_all = (val > hi) | (val < lo)

        st.session_state["cpu_abn_count"] = int(ab_mask_all.fillna(False).sum())
        st.session_state["cpu_status"]    = "Abnormal" if ab_mask_all.any() else "Normal"

        # ✅ เก็บ abnormal ทั้งหมด
        self.df_abnormal = df_result.loc[ab_mask_all].copy()

        # 7) Styled main table
        styled = self._style_dataframe(df_filtered.copy())
        st.markdown("### CPU Performance")
        st.write(styled)

        # 8) Summary banner
        failed_rows = df_filtered.apply(
            lambda r: self._row_has_issue(r, self.COL_VAL, self.COL_MIN, self.COL_MAX),
            axis=1
        )
        st.markdown(
            "<div style='text-align:center; font-size:32px; font-weight:bold; color:{};'>CPU Performance {}</div>".format(
                "red" if failed_rows.any() else "green",
                "Warning" if failed_rows.any() else "Normal"
            ),
            unsafe_allow_html=True
        )

        # 9) Site-Obj column
        df_result["Site-Obj"] = (
            df_result["Site Name"].astype(str) + " - " + df_result[self.COL_MOBJ].astype(str)
        )

        # 10) Subsets
        df_snp  = df_result[df_result[self.COL_MOBJ].str.contains(r"SNP\(E\)")].copy()
        df_ncpm = df_result[df_result[self.COL_MOBJ].str.contains(r"NCPM")].copy()
        df_ncpq = df_result[df_result[self.COL_MOBJ].str.contains(r"NCPQ")].copy()

        # 11) CPU% (คูณ 100 เพราะไฟล์ต้นทางเป็น ratio)
        for df_sub in [df_snp, df_ncpm, df_ncpq]:
            df_sub["CPU%"] = pd.to_numeric(df_sub[self.COL_VAL], errors="coerce") * 100

        # ✅ เก็บ abnormal แยกตาม type
        self.df_abnormal_by_type = {}
        for btype, df_sub in {"SNP(E)": df_snp, "NCPM": df_ncpm, "NCPQ": df_ncpq}.items():
            v  = pd.to_numeric(df_sub[self.COL_VAL], errors="coerce")
            hi = pd.to_numeric(df_sub[self.COL_MAX], errors="coerce")
            lo = pd.to_numeric(df_sub[self.COL_MIN], errors="coerce")
            ab_mask = (v > hi) | (v < lo)
            if ab_mask.any():
                self.df_abnormal_by_type[btype] = df_sub.loc[ab_mask].copy()

        # 12) Global X scale
        global_max = max(df_snp["CPU%"].max(), df_ncpm["CPU%"].max(), df_ncpq["CPU%"].max())
        x_max = (global_max or 0) * 1.1  # กันชน 10%

        # ---------- Helpers ----------
        def plot_chart(df_sub: pd.DataFrame, title: str, height: int):
            # ✅ เรียงจากมากไปน้อย
            df_sub = df_sub.sort_values(by="CPU%", ascending=False).copy()
            df_sub["Status"] = df_sub["CPU%"].apply(lambda x: "Overload" if x > 90 else "Normal")

            # ✅ กำหนด order ของ Y-axis ตาม DataFrame ที่เรียงแล้ว
            y_order = df_sub["Site-Obj"].tolist()

            chart_bar = alt.Chart(df_sub).mark_bar().encode(
                x=alt.X("CPU%", title="CPU utilization (%)",
                        scale=alt.Scale(domain=[0, x_max])),
                y=alt.Y("Site-Obj", sort=y_order, title="Site - Measure Object",
                        axis=alt.Axis(labelLimit=0)),
                color=alt.Color("Status",
                                scale=alt.Scale(domain=["Normal", "Overload"],
                                                range=["green", "red"]))
            ).properties(title=title, width=900, height=height)

            chart_text = alt.Chart(df_sub).mark_text(
                align="left", baseline="middle", dx=3
            ).encode(
                x="CPU%",
                y=alt.Y("Site-Obj", sort=y_order),
                text=alt.Text("CPU%:Q", format=".1f")
            )

            return chart_bar + chart_text

        def show_abnormal(df_sub: pd.DataFrame, title: str):
            st.markdown(f"#### {title} – Abnormal Rows")
            v  = pd.to_numeric(df_sub[self.COL_VAL], errors="coerce") * 100
            hi = pd.to_numeric(df_sub[self.COL_MAX], errors="coerce") * 100
            lo = pd.to_numeric(df_sub[self.COL_MIN], errors="coerce") * 100
            ab_mask = (v > hi) | (v < lo)

            if not ab_mask.any():
                st.info("✅ No abnormal rows (Normal)")
                return

            df_abn = df_sub.loc[ab_mask, ["Site Name", self.COL_ME, self.COL_MOBJ,
                                        self.COL_MAX, self.COL_MIN, self.COL_VAL]].copy()
            df_abn[self.COL_VAL] = v.loc[ab_mask]
            df_abn[self.COL_MAX] = hi.loc[ab_mask]
            df_abn[self.COL_MIN] = lo.loc[ab_mask]

            # 🔹 Format เป็น %
            df_abn = df_abn.rename(columns={
                self.COL_VAL: "CPU utilization (%)",
                self.COL_MAX: "Maximum threshold (%)",
                self.COL_MIN: "Minimum threshold (%)"
            })

            # 🔹 Format % เฉพาะ 3 คอลัมน์นี้
            percent_cols = ["CPU utilization (%)", "Maximum threshold (%)", "Minimum threshold (%)"]
            for col in percent_cols:
                df_abn[col] = pd.to_numeric(df_abn[col], errors="coerce").round(1).astype(str) + "%"

            # ✅ Highlight CPU utilization (%) เป็นสีแดง
            def highlight_red(val):
                try:
                    v = float(val.strip('%'))
                    return "background-color: #ff4d4d; color: white"
                except:
                    return ""

            styled_abn = (
                df_abn.style
                .applymap(highlight_red, subset=["CPU utilization (%)"])
            )

            st.dataframe(styled_abn, use_container_width=True)

        # ---------- /Helpers ----------

        # 13) SNP(E)
        st.markdown(f"#### CPU Performance – SNP(E) Board")

        rows = len(df_snp)
        height_full = min(rows * 30, 2000)   # full chart
        height_preview = 400                 # preview chart

        tab1, tab2 = st.tabs(["🔎 Preview (Top10)", "📊 Full chart"])
        with tab1:
            df_top10 = df_snp.sort_values(by="CPU%", ascending=False).head(10)
            st.altair_chart(plot_chart(df_top10, "SNP(E) CPU Utilization (Top 10)", height_preview),
                            use_container_width=True)
        with tab2:
            st.altair_chart(plot_chart(df_snp, "SNP(E) CPU Utilization (~100 Boards)", height_full),
                            use_container_width=True)

        show_abnormal(df_snp, "SNP(E)")
        st.markdown("<br><br><br>", unsafe_allow_html=True)

        # 14) NCPM
        st.markdown(f"#### CPU Performance – NCPM Board")
        st.altair_chart(
            plot_chart(df_ncpm, "NCPM CPU Utilization (8 Boards)", 400),
            use_container_width=True
        )
        show_abnormal(df_ncpm, "NCPM")
        st.markdown("<br><br><br>", unsafe_allow_html=True)

        # 15) NCPQ
        st.markdown(f"#### CPU Performance – NCPQ Board")
        st.altair_chart(plot_chart(df_ncpq, "NCPQ CPU Utilization (16 Boards)", 600),
                        use_container_width=True)
        show_abnormal(df_ncpq, "NCPQ")
        st.markdown("<br><br><br>", unsafe_allow_html=True)

        # ✅ เก็บ analyzer object ลง session
        st.session_state["cpu_analyzer"] = self

        return df_result


    def prepare(self) -> None:
        """เตรียมข้อมูล abnormal โดยไม่ render UI และไม่ใช้ cascading_filter"""
        # 1) Normalize
        self.df_cpu = self._normalize_columns(self.df_cpu)
        self.df_ref = self._normalize_columns(self.df_ref)

        # 2) Check required columns
        self._check_required()
        self._check_required_ref()

        # 3) Merge กับ reference
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            self.df_abnormal = pd.DataFrame()
            self.df_abnormal_by_type = {}
            return

        # 4) Detect abnormal
        val = pd.to_numeric(df_merged[self.COL_VAL], errors="coerce")
        hi  = pd.to_numeric(df_merged[self.COL_MAX], errors="coerce")
        lo  = pd.to_numeric(df_merged[self.COL_MIN], errors="coerce")
        ab_mask = (val > hi) | (val < lo)

        df_abn = df_merged.loc[ab_mask, [
            "Site Name", self.COL_ME, self.COL_MOBJ,
            self.COL_MAX, self.COL_MIN, self.COL_VAL
        ]].copy()

        # 5) เก็บผล
        self.df_abnormal = df_abn
        self.df_abnormal_by_type = {"All": df_abn} if not df_abn.empty else {}
