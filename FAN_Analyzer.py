import pandas as pd
import streamlit as st
from utils.filters import cascading_filter
import altair as alt
import re


class FAN_Analyzer:
    """
    วิเคราะห์ FAN:
      - ตรวจคอลัมน์ที่ต้องมี
      - สร้าง Mapping Format แล้ว merge กับ reference
      - เรียงตาม order จาก reference
      - filter แบบ cascading_filter
      - ไฮไลต์ค่าที่ผิดตามกฎ FCC/FCPP/FCPL/FCPS
      - สรุปสถานะ Warning/Normal
    """

    def __init__(self, df_fan: pd.DataFrame, df_ref: pd.DataFrame, ns: str = "fan"):
        self.df_fan = df_fan
        self.df_ref = df_ref
        self.ns = ns

        self.df_abnormal = pd.DataFrame()   # abnormal table
        self.df_abnormal_by_type = {}       # abnormal table แยกตาม FanType

        # ชื่อคอลัมน์หลัก
        self.COL_ME = "ME"
        self.COL_MOBJ = "Measure Object"
        self.COL_BEGIN = "Begin Time"
        self.COL_END = "End Time"
        self.COL_VALUE = "Value of Fan Rotate Speed(Rps)"
        self.COL_MAX_TH = "Maximum threshold"
        self.COL_MIN_TH = "Minimum threshold"

    # ---------- Utilities ----------
    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.astype(str)
            .str.strip().str.replace(r"\s+", " ", regex=True)
            .str.replace("\u00a0", " ")
        )
        return df

    @staticmethod
    def extract_board(mobj: str) -> str:
        if not isinstance(mobj, str):
            return ""
        return re.sub(r"-Fan\[.*\]", "", mobj)

    @staticmethod
    def extract_port(mobj: str) -> str:
        if not isinstance(mobj, str):
            return ""
        m = re.search(r"FanID:(\d+)", mobj)
        return m.group(1) if m else ""

    def _check_required(self) -> None:
        required_cols = {self.COL_ME, self.COL_MOBJ, self.COL_BEGIN, self.COL_END, self.COL_VALUE}
        missing = required_cols - set(self.df_fan.columns)
        if missing:
            raise ValueError(f"Uploaded file must contain columns: {', '.join(sorted(required_cols))}")

    def _merge_with_ref(self) -> pd.DataFrame:
        self.df_fan["Mapping Format"] = (
            self.df_fan[self.COL_ME].astype(str).str.strip()
            + self.df_fan[self.COL_MOBJ].astype(str).str.strip()
        )

        df_ref_subset = self.df_ref[["Mapping", "Site Name", self.COL_MAX_TH, self.COL_MIN_TH]].copy()
        df_ref_subset["Mapping"] = df_ref_subset["Mapping"].astype(str).str.strip()
        df_ref_subset["order"] = range(len(df_ref_subset))

        df_merged = pd.merge(
            self.df_fan,
            df_ref_subset,
            left_on="Mapping Format",
            right_on="Mapping",
            how="inner"
        )
        return df_merged

    @staticmethod
    def _is_not_ok_rule(measure_object: str, value) -> bool:
        try:
            v = float(value)
        except Exception:
            return False

        mo = str(measure_object)
        if "FCC" in mo and v > 120:
            return True
        if "FCPP" in mo and v > 250:
            return True
        if "FCPL" in mo and v > 120:
            return True
        if "FCPS" in mo and v > 230:
            return True
        return False

    def _style_dataframe(self, df_view: pd.DataFrame):
        if self.COL_VALUE in df_view.columns:
            df_view[self.COL_VALUE] = pd.to_numeric(df_view[self.COL_VALUE], errors="coerce")

        highlight_mask = df_view.apply(
            lambda r: self._is_not_ok_rule(r[self.COL_MOBJ], r[self.COL_VALUE]),
            axis=1
        )

        def gray_row(r):
            return ['background-color:#e6e6e6;color:black' if highlight_mask.iloc[r.name] else '' for _ in r]

        def red_value(_):
            return ['background-color:#ff4d4d;color:white' if m else '' for m in highlight_mask]

        styled_df = (
            df_view.style
            .apply(gray_row, axis=1)
            .apply(red_value, subset=[self.COL_VALUE] if self.COL_VALUE in df_view.columns else [])
            .format({self.COL_VALUE: "{:.2f}"})
        )
        return styled_df, highlight_mask

    # ---------- Chart ----------
    def _plot_chart(self, df_sub: pd.DataFrame, ftype: str, height: int, th: float):
        df_sub = df_sub.sort_values(by="Avg Fan Speed (Rps)", ascending=False).copy()
        df_sub["Status"] = df_sub["Avg Fan Speed (Rps)"].apply(
            lambda x: "Abnormal" if x > th else "Normal"
        )

        chart_bar = alt.Chart(df_sub).mark_bar().encode(
            x=alt.X("Avg Fan Speed (Rps)", title="Fan Speed (Rps)",
                    scale=alt.Scale(domain=[0, df_sub["Avg Fan Speed (Rps)"].max() * 1.1])),
            y=alt.Y("Site-Obj", sort=None, title="Site - Board", axis=alt.Axis(labelLimit=0)),
            color=alt.Color("Status",
                            scale=alt.Scale(domain=["Normal", "Abnormal"],
                                            range=["green", "red"]))
        ).properties(width=900, height=height)

        chart_text = alt.Chart(df_sub).mark_text(
            align="left", baseline="middle", dx=3
        ).encode(
            x="Avg Fan Speed (Rps)",
            y=alt.Y("Site-Obj", sort=None),
            text=alt.Text("Avg Fan Speed (Rps):Q", format=".2f")
        )
        return chart_bar + chart_text

    # ---------- MAIN ----------
    def process(self) -> pd.DataFrame:
        self.df_abnormal = pd.DataFrame()
        self.df_abnormal_by_type = {}

        # Normalize
        self.df_fan = self._normalize_columns(self.df_fan)
        self.df_ref = self._normalize_columns(self.df_ref)

        # Required check
        self._check_required()

        # Merge with reference
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            st.info("No matching mapping found between FAN file and reference")
            return pd.DataFrame()

        # Build df_result
        df_result = df_merged[[
            self.COL_BEGIN, self.COL_END, "Site Name", self.COL_ME, self.COL_MOBJ,
            self.COL_MAX_TH, self.COL_MIN_TH, self.COL_VALUE, "order"
        ]].copy()

        df_result = df_result.sort_values("order").drop(columns=["order"]).reset_index(drop=True)

        # Filtering
        df_filtered, _sel = cascading_filter(
            df_result,
            cols=["Site Name", self.COL_ME, self.COL_MOBJ],
            ns=self.ns,
            clear_text="Clear FAN Filters"
        )
        st.caption(f"FAN (showing {len(df_filtered)}/{len(df_result)} rows)")

        # Style table
        styled_df, highlight_mask = self._style_dataframe(df_filtered.copy())
        st.markdown("### FAN Performance (Main Table)")
        st.dataframe(styled_df, use_container_width=True)

        # Status text
        st.markdown(
            "<div style='text-align:center; font-size:32px; font-weight:bold; color:{};'>FAN Performance {}</div>".format(
                "red" if highlight_mask.any() else "green",
                "Warning" if highlight_mask.any() else "Normal"
            ),
            unsafe_allow_html=True
        )
        st.markdown("<br><br>", unsafe_allow_html=True)

        # Add FanType, Board, Port
        df_result["FanType"] = df_result[self.COL_MOBJ].str.extract(r"(FCC|FCPP|FCPL|FCPS)")
        df_result["Board"] = df_result[self.COL_MOBJ].apply(self.extract_board)
        df_result["Port"] = df_result[self.COL_MOBJ].apply(self.extract_port)

        # Average by group
        df_avg = (
            df_result
            .groupby(["FanType", self.COL_ME, "Site Name", "Board"], as_index=False)[self.COL_VALUE]
            .mean()
            .rename(columns={self.COL_VALUE: "Avg Fan Speed (Rps)"})
        )
        df_avg["Site-Obj"] = df_avg["Site Name"].astype(str) + " - " + df_avg["Board"].astype(str)

        # Thresholds
        thresholds = {"FCC": 120, "FCPP": 250, "FCPL": 120, "FCPS": 230}

        # Abnormal table (per FanType)
        def show_abnormal_from_main(df_main: pd.DataFrame, title: str):
            st.markdown(f"#### {title} – Abnormal Rows")
            ab_mask = df_main.apply(
                lambda r: self._is_not_ok_rule(r[self.COL_MOBJ], r[self.COL_VALUE]),
                axis=1
            )

            if not ab_mask.any():
                st.info(" No abnormal rows (Normal)")
                return

            df_abn = df_main.loc[ab_mask, [
                "Site Name", self.COL_ME, self.COL_MOBJ,
                self.COL_MAX_TH, self.COL_MIN_TH, self.COL_VALUE
            ]].copy()

            df_abn[self.COL_VALUE] = pd.to_numeric(df_abn[self.COL_VALUE], errors="coerce").round(2)
            df_abn[self.COL_MAX_TH] = pd.to_numeric(df_abn[self.COL_MAX_TH], errors="coerce").round(2)
            df_abn[self.COL_MIN_TH] = pd.to_numeric(df_abn[self.COL_MIN_TH], errors="coerce").round(2)

            # เก็บ abnormal
            self.df_abnormal = pd.concat([self.df_abnormal, df_abn], ignore_index=True)
            self.df_abnormal_by_type[ftype] = df_abn.copy()

            st.write("DEBUG FAN_Analyzer df_abnormal_by_type keys:", list(self.df_abnormal_by_type.keys()))

            # highlight Value column
            def highlight_red(val):
                try:
                    v = float(val)
                    return "background-color: #ff4d4d; color: white" if v > 0 else ""
                except Exception:
                    return ""

            styled_abn = (
                df_abn.style
                .applymap(highlight_red, subset=[self.COL_VALUE])
                .format({self.COL_VALUE: "{:.2f}"})
            )
            st.dataframe(styled_abn, use_container_width=True)

        # Loop per FanType
        for ftype, th in thresholds.items():
            df_sub = df_avg[df_avg["FanType"] == ftype].copy()
            if df_sub.empty:
                continue

            rows = len(df_sub)
            height_full = min(rows * 30, 2000)
            height_preview = 400

            if ftype in ["FCC", "FCPL", "FCPS"]:
                tab1, tab2 = st.tabs(["🔎 Preview (Top10)", "📊 Full chart"])
                with tab1:
                    df_top10 = df_sub.sort_values(by="Avg Fan Speed (Rps)", ascending=False).head(10)
                    st.altair_chart(self._plot_chart(df_top10, ftype, height_preview, th),
                                    use_container_width=True)
                with tab2:
                    st.altair_chart(self._plot_chart(df_sub, ftype, height_full, th),
                                    use_container_width=True)
            else:  # FCPP
                st.altair_chart(self._plot_chart(df_sub, ftype, height_full, th),
                                use_container_width=True)

            # abnormal table
            show_abnormal_from_main(df_result[df_result[self.COL_MOBJ].str.contains(ftype)], ftype)
            st.markdown("<br><br><br><br>", unsafe_allow_html=True)

        st.write("DEBUG df_abnormal", self.df_abnormal)
        return df_result
    
    def prepare(self) -> pd.DataFrame:
        """
        เตรียมข้อมูล FAN สำหรับ Summary (ไม่ render UI)
        return df_result ที่ merge แล้ว พร้อม abnormal เก็บใน self
        """
        # 1) Normalize
        self.df_fan = self._normalize_columns(self.df_fan)
        self.df_ref = self._normalize_columns(self.df_ref)

        # 2) Required check
        self._check_required()

        # 3) Merge with reference
        df_merged = self._merge_with_ref()
        if df_merged.empty:
            return pd.DataFrame()

        # 4) Build df_result
        df_result = df_merged[[
            self.COL_BEGIN, self.COL_END, "Site Name", self.COL_ME, self.COL_MOBJ,
            self.COL_MAX_TH, self.COL_MIN_TH, self.COL_VALUE, "order"
        ]].copy()

        df_result = df_result.sort_values("order").drop(columns=["order"]).reset_index(drop=True)

        # 5) Add FanType, Board, Port
        df_result["FanType"] = df_result[self.COL_MOBJ].str.extract(r"(FCC|FCPP|FCPL|FCPS)")
        df_result["Board"] = df_result[self.COL_MOBJ].apply(self.extract_board)
        df_result["Port"] = df_result[self.COL_MOBJ].apply(self.extract_port)

        # 6) Detect abnormal (รวมทั้งหมด)
        ab_mask_all = df_result.apply(
            lambda r: self._is_not_ok_rule(r[self.COL_MOBJ], r[self.COL_VALUE]),
            axis=1
        )

        self.df_abnormal = df_result.loc[ab_mask_all].copy()

        # 7) Detect abnormal แยกตาม FanType
        self.df_abnormal_by_type = {}
        thresholds = {"FCC": 120, "FCPP": 250, "FCPL": 120, "FCPS": 230}
        for ftype, th in thresholds.items():
            df_sub = df_result[df_result["FanType"] == ftype].copy()
            if df_sub.empty:
                continue

            ab_mask = df_sub.apply(
                lambda r: self._is_not_ok_rule(r[self.COL_MOBJ], r[self.COL_VALUE]),
                axis=1
            )
            if ab_mask.any():
                self.df_abnormal_by_type[ftype] = df_sub.loc[ab_mask].copy()

        return df_result
