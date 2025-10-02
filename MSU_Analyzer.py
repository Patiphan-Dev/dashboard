import pandas as pd
import streamlit as st
from utils.filters import cascading_filter

class MSU_Analyzer:
    """
    วิเคราะห์ MSU:
      - ตรวจคอลัมน์ที่ต้องมี
      - สร้าง Mapping Format แล้ว merge กับ reference
      - เรียงตาม order จาก reference
      - filter แบบ cascading_filter
      - ไฮไลต์สีแดงถ้า Laser Bias Current > Threshold
      - สรุปสถานะ Warning/Normal
      - Visualization: Bar Chart
    """

    def __init__(self, df_msu: pd.DataFrame, df_ref: pd.DataFrame, ns: str = "msu"):
        self.df_msu = df_msu
        self.df_ref = df_ref
        self.ns     = ns

        # column name mapping
        self.COL_ME    = "ME"
        self.COL_MOBJ  = "Measure Object"
        self.COL_LASER = "Laser Bias Current(mA)"
        self.COL_TH    = "Maximum threshold"

        # abnormal data containers
        self.df_abnormal = pd.DataFrame()
        self.df_abnormal_by_type = {}

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
        required_cols = {self.COL_ME, self.COL_MOBJ, self.COL_LASER}
        missing = required_cols - set(self.df_msu.columns)
        if missing:
            raise ValueError(f"MSU file must contain columns: {', '.join(sorted(required_cols))}")

    def _check_required_ref(self) -> None:
        required_ref_cols = {"Site Name", "Mapping", self.COL_TH}
        missing = required_ref_cols - set(self.df_ref.columns)
        if missing:
            raise ValueError(f"Reference file must contain columns: {', '.join(sorted(required_ref_cols))}")

    def _merge_with_ref(self) -> pd.DataFrame:
        self.df_msu["Mapping Format"] = (
            self.df_msu[self.COL_ME].astype(str).str.strip()
            + self.df_msu[self.COL_MOBJ].astype(str).str.strip()
        )
        self.df_ref["Mapping"] = self.df_ref["Mapping"].astype(str).str.strip()
        self.df_ref["order"]   = range(len(self.df_ref))

        df_merged = pd.merge(
            self.df_msu,
            self.df_ref[["Site Name", "Mapping", self.COL_TH, "order"]],
            left_on="Mapping Format",
            right_on="Mapping",
            how="inner"
        )
        return df_merged

    def _style_dataframe(self, df_view: pd.DataFrame) -> pd.io.formats.style.Styler:
        # แปลงเป็น numeric
        for c in [self.COL_LASER, self.COL_TH]:
            if c in df_view.columns:
                df_view[c] = pd.to_numeric(df_view[c], errors="coerce")

        # ✅ ไฮไลต์คอลัมน์ Laser ถ้าเกิน threshold
        def red_value(_):
            return [
                "background-color:#ff4d4d;color:white"
                if (pd.notna(v) and pd.notna(th) and v > th)
                else ""
                for v, th in zip(
                    df_view.get(self.COL_LASER, pd.Series(index=df_view.index)),
                    df_view.get(self.COL_TH, pd.Series(index=df_view.index)),
                )
            ]

        styled = (
            df_view.style
            .apply(red_value, subset=[self.COL_LASER] if self.COL_LASER in df_view.columns else [])
            .format({
                self.COL_LASER: "{:.2f}",
                self.COL_TH: "{:.2f}",
            })
        )
        return styled

    # ---------- MAIN ----------
    def process(self) -> None:
        # 1) Normalize
        self.df_msu = self._normalize_columns(self.df_msu)
        self.df_ref = self._normalize_columns(self.df_ref)

        # 2) Check required
        self._check_required()
        self._check_required_ref()

        # 3) Merge
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            st.warning("No matching mapping found between MSU file and reference")
            self.df_abnormal = pd.DataFrame()
            self.df_abnormal_by_type = {}
            return

        # 4) Pick columns
        df_result = (
            df_merged[["Site Name", self.COL_ME, self.COL_MOBJ, self.COL_TH, self.COL_LASER, "order"]]
            .sort_values("order")
            .drop(columns=["order"])
            .reset_index(drop=True)
        )

        # 5) Cascading filter
        df_filtered, _sel = cascading_filter(
            df_result,
            cols=["Site Name", self.COL_ME, self.COL_MOBJ],
            ns=self.ns,
            clear_text="Clear MSU Filters"
        )
        st.caption(f"MSU (showing {len(df_filtered)}/{len(df_result)} rows)")

        # 6) Main table (ใช้ Styler + format 2 ตำแหน่ง)
        styled_main = self._style_dataframe(df_filtered.copy())
        st.markdown("### MSU Performance")
        st.write(styled_main)

        # 7) Summary banner
        failed_rows = df_filtered[self.COL_LASER] > df_filtered[self.COL_TH]
        st.markdown(
            "<div style='text-align:center; font-size:32px; font-weight:bold; color:{};'>MSU Performance {}</div>".format(
                "red" if failed_rows.any() else "green",
                "Warning" if failed_rows.any() else "Normal"
            ),
            unsafe_allow_html=True
        )

        # 8) Visualization ------------------
        import plotly.express as px
        df_board = df_result.copy()
        df_board["Board"] = df_board["Site Name"].astype(str) + " | " + df_board[self.COL_MOBJ].astype(str)

        df_board["Status"] = df_board.apply(
            lambda r: "Normal" if r[self.COL_LASER] <= r[self.COL_TH] else "Abnormal", axis=1
        )

        view_option = st.radio(
            "View Option:",
            ["Show All Ports", "Active Only (Laser > 0)"],
            index=1,
            horizontal=True
        )
        if view_option == "Active Only (Laser > 0)":
            df_board = df_board[df_board[self.COL_LASER] > 0]

        total_ports    = len(df_result)
        active_ports   = (df_result[self.COL_LASER] > 0).sum()
        abnormal_ports = (df_result[self.COL_LASER] > df_result[self.COL_TH]).sum()

        st.markdown(
            f"""
            <div style="text-align:center; font-size:18px; font-weight:bold;">
                Total Ports: {total_ports} |
                Active: {active_ports} |
                Abnormal: {abnormal_ports}
            </div>
            """,
            unsafe_allow_html=True
        )

        fig_bar = px.bar(
            df_board.sort_values(self.COL_LASER, ascending=False),
            x=self.COL_LASER, y="Board",
            orientation="h", color="Status",
            color_discrete_map={"Normal": "#5dcb61", "Abnormal": "red"},
            text=self.COL_LASER
        )
        fig_bar.add_vline(
            x=df_board[self.COL_TH].iloc[0],
            line_dash="dash", line_color="blue",
            annotation_text="Threshold", annotation_position="top right"
        )
        fig_bar.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig_bar.update_layout(
            title="MSU Laser Bias Current vs Threshold",
            xaxis_title="Laser Bias Current (mA)",
            yaxis_title="Site | Board",
            yaxis=dict(autorange="reversed", tickfont=dict(size=14)),
            font=dict(size=13),
            height=20 * len(df_board),
            bargap=0.1
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # 9) Abnormal Table ------------------
        ab_mask = df_result[self.COL_LASER] > df_result[self.COL_TH]
        df_abn = df_result.loc[ab_mask, [
            "Site Name", self.COL_ME, self.COL_MOBJ,
            self.COL_TH, self.COL_LASER
        ]].copy()

        if not df_abn.empty:
            # ✅ round 2 decimal (ไม่มีหน่วย)
            df_abn[self.COL_TH]    = pd.to_numeric(df_abn[self.COL_TH], errors="coerce").round(2)
            df_abn[self.COL_LASER] = pd.to_numeric(df_abn[self.COL_LASER], errors="coerce").round(2)

            # ✅ Highlight Laser Bias Current(mA)
            def highlight_red(val):
                try:
                    return "background-color: #ff4d4d; color: white" if float(val) > 0 else ""
                except:
                    return ""

            styled_abn = (
                df_abn.style
                .applymap(highlight_red, subset=[self.COL_LASER])
                .format({
                    self.COL_LASER: "{:.2f}",
                    self.COL_TH: "{:.2f}",
                })
            )

            st.markdown("#### MSU – Abnormal Rows")
            st.dataframe(styled_abn, use_container_width=True)
        else:
            st.info("✅ No abnormal rows (Normal)")

        # 10) เก็บ abnormal ลง property (ใช้ใน summary/PDF)
        self.df_abnormal = df_abn
        self.df_abnormal_by_type = {"MSU": df_abn} if not df_abn.empty else {}

 

    # ---------- PREPARE ----------
    def prepare(self) -> None:
        """เตรียมข้อมูล abnormal โดยไม่ render UI"""
        # 1) Normalize
        self.df_msu = self._normalize_columns(self.df_msu)
        self.df_ref = self._normalize_columns(self.df_ref)

        # 2) Check required
        self._check_required()
        self._check_required_ref()

        # 3) Merge
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            self.df_abnormal = pd.DataFrame()
            self.df_abnormal_by_type = {}
            st.session_state["msu_analyzer"] = self
            st.session_state["msu_status"]   = "No data"
            st.session_state["msu_abn_count"] = 0
            return

        # 4) Detect abnormal
        val = pd.to_numeric(df_merged[self.COL_LASER], errors="coerce")
        th  = pd.to_numeric(df_merged[self.COL_TH], errors="coerce")
        ab_mask = val > th

        df_abn = df_merged.loc[ab_mask, [
            "Site Name", self.COL_ME, self.COL_MOBJ,
            self.COL_TH, self.COL_LASER
        ]].copy()

        # 5) เก็บผล
        self.df_abnormal = df_abn
        self.df_abnormal_by_type = {"MSU": df_abn} if not df_abn.empty else {}

        # ✅ เก็บเข้า session_state
        st.session_state["msu_analyzer"] = self
        st.session_state["msu_status"]   = "Abnormal" if not df_abn.empty else "Normal"
        st.session_state["msu_abn_count"] = len(df_abn)
