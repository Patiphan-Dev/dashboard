import re
from collections import OrderedDict  # NEW: สำหรับเก็บตารางรายวันแบบเรียงลำดับ
import pandas as pd
import streamlit as st
import plotly.express as px
from utils.filters import cascading_filter

# หมายเหตุ: ต้องมีฟังก์ชัน cascading_filter(df, cols, ns, labels=None, clear_text="...") อยู่ภายนอกให้เรียกใช้งานได้


class FiberflappingAnalyzer:
    """
    จัดระเบียบ logic สำหรับ Fiber Flapping:
      - เตรียม/normalize ข้อมูล OSC Optical และ FM
      - กรองรายการที่ Max-Min(dB) > threshold
      - หาแถวที่ 'ไม่เจอ alarm match'
      - เรนเดอร์ตาราง highlight + KPI รายวัน + กราฟรวม 7 วัน

    การใช้งาน:
        analyzer = FiberflappingAnalyzer(df_optical, df_fm, threshold=2.0)
        analyzer.process()
    """

    def __init__(self, df_optical: pd.DataFrame, df_fm: pd.DataFrame, threshold: float = 2.0):
        self.df_optical_raw = df_optical
        self.df_fm_raw = df_fm
        self.threshold = threshold
        self.daily_tables = None  # NEW: เก็บผลตารางรายวันสำหรับ export/report

     

    # -------------------- Normalize / Prepare --------------------
    def normalize_optical(self) -> pd.DataFrame:
        df = self.df_optical_raw.copy()
        df.columns = df.columns.str.strip()

        # คำนวณ Max - Min (dB)
        df["Max - Min (dB)"] = (
            df["Max Value of Input Optical Power(dBm)"]
            - df["Min Value of Input Optical Power(dBm)"]
        )

        # Extract Target ME จาก Measure Object: ค่าในวงเล็บ
        def extract_target(measure_obj):
            match = re.search(r"\(([^)]+)\)", str(measure_obj))
            return match.group(1) if match else None

        df["Target ME"] = df["Measure Object"].apply(extract_target)

        # เวลาช่วง Begin/End
        df["Begin Time"] = pd.to_datetime(df["Begin Time"], errors="coerce")
        df["End Time"] = pd.to_datetime(df["End Time"], errors="coerce")
        return df

    def normalize_fm(self) -> tuple[pd.DataFrame, str]:
        df = self.df_fm_raw.copy()
        df.columns = df.columns.str.strip()

        df["Occurrence Time"] = pd.to_datetime(df["Occurrence Time"], errors="coerce")
        df["Clear Time"] = pd.to_datetime(df["Clear Time"], errors="coerce")

        # หา column แรกที่ขึ้นต้นด้วย "Link"
        link_cols = [c for c in df.columns if c.startswith("Link")]
        if not link_cols:
            raise ValueError("No 'Link*' column found in FM Alarm file.")
        link_col = link_cols[0]
        return df, link_col

    # -------------------- Core Filtering --------------------
    def filter_optical_by_threshold(self, df_optical_norm: pd.DataFrame) -> pd.DataFrame:
        return df_optical_norm[df_optical_norm["Max - Min (dB)"] > self.threshold].copy()

    def find_nomatch(self, df_filtered: pd.DataFrame, df_fm_norm: pd.DataFrame, link_col: str) -> pd.DataFrame:
        """
        หาแถวใน df_filtered ที่ 'ไม่เจอ' alarm match:
          - Link column ใน FM ต้อง contains ทั้ง ME และ Target ME
          - และช่วงเวลา overlap: Occurrence <= End และ Clear >= Begin
        """
        result_rows = []
        for _, row in df_filtered.iterrows():
            me = re.escape(str(row.get("ME", "")))
            target_me = re.escape(str(row.get("Target ME", "")))
            begin_t = row.get("Begin Time", pd.NaT)
            end_t = row.get("End Time", pd.NaT)

            # เงื่อนไขเวลา (ถ้า NaT จะไม่ match)
            matched = df_fm_norm[
                df_fm_norm[link_col].astype(str).str.contains(me, na=False)
                & df_fm_norm[link_col].astype(str).str.contains(target_me, na=False)
                & (df_fm_norm["Occurrence Time"] <= end_t)
                & (df_fm_norm["Clear Time"] >= begin_t)
            ]

            if matched.empty:
                result_rows.append(row)

        return pd.DataFrame(result_rows)

    # -------------------- View Preparation --------------------
    @staticmethod
    def prepare_view(df_nomatch: pd.DataFrame) -> pd.DataFrame:
        # คอลัมน์ที่จะโชว์
        view_cols = [
            "Begin Time", "End Time", "Granularity", "ME", "ME IP", "Measure Object",
            "Max Value of Input Optical Power(dBm)", "Min Value of Input Optical Power(dBm)",
            "Input Optical Power(dBm)", "Max - Min (dB)"
        ]
        view_cols = [c for c in view_cols if c in df_nomatch.columns]
        df_view = df_nomatch[view_cols].copy()

        # แปลงตัวเลขเพื่อ format ทศนิยม
        num_cols = [
            "Max Value of Input Optical Power(dBm)",
            "Min Value of Input Optical Power(dBm)",
            "Input Optical Power(dBm)",
            "Max - Min (dB)"
        ]
        num_cols = [c for c in num_cols if c in df_view.columns]
        if num_cols:
            df_view.loc[:, num_cols] = df_view[num_cols].apply(pd.to_numeric, errors="coerce")
        return df_view

    # -------------------- Rendering --------------------
    def render(self, df_nomatch: pd.DataFrame) -> None:
        st.markdown("### OSC Power Flapping (No Alarm Match)")

        if df_nomatch.empty:
            st.success("No unmatched fiber flapping records found")
            return

        # Cascading filter
        df_nomatch_filtered, _sel = cascading_filter(
            df_nomatch,
            cols=["ME", "Measure Object"],
            ns="fiber",
            labels={"ME": "Managed Element"},
            clear_text="Clear Fiber Filters",
        )
        st.caption(f"Fiber Flapping (showing {len(df_nomatch_filtered)}/{len(df_nomatch)} rows)")

        # เตรียมตารางแสดงผล
        df_view = self.prepare_view(df_nomatch_filtered)

        # Highlight เฉพาะคอลัมน์ "Max - Min (dB)" > threshold
        if "Max - Min (dB)" in df_view.columns:
            styled_df = (
                df_view.style
                .apply(
                    lambda _:
                        ['background-color:#ff4d4d; color:white' if (v > self.threshold) else ''
                         for v in df_view["Max - Min (dB)"]],
                    subset=["Max - Min (dB)"]
                )
                .format({
                    "Max Value of Input Optical Power(dBm)": "{:.2f}",
                    "Min Value of Input Optical Power(dBm)": "{:.2f}",
                    "Input Optical Power(dBm)": "{:.2f}",
                    "Max - Min (dB)": "{:.2f}",
                })
            )
            st.write(styled_df)
            
        else:
            st.dataframe(df_view, use_container_width=True)
        
        # คืนค่า view
        return df_view

    # -------------------- Weekly KPI (7-Day Summary) --------------------
    def render_weekly_summary(self, df_nomatch: pd.DataFrame) -> None:
        """Summary KPI: Flapping sites per day (7-day view with drill-down + graph at the end)"""
        if df_nomatch.empty:
            st.success("No unmatched fiber flapping records in past 7 days")
            return

        df_nomatch = df_nomatch.copy()
        df_nomatch["Date"] = pd.to_datetime(df_nomatch["Begin Time"]).dt.date

        # หาช่วงวัน start → end
        start_date = df_nomatch["Date"].min()
        end_date   = df_nomatch["Date"].max()
        st.markdown(f"### Fiber Flapping Summary (Past 7 Days: {start_date} → {end_date})")

        # นับจำนวน site ต่อวัน
        daily_counts = (
            df_nomatch.groupby("Date")["ME"].nunique().reset_index()
            .rename(columns={"ME": "Sites"})
        )

        # เก็บวันที่เลือก
        if "selected_day" not in st.session_state:
            st.session_state["selected_day"] = None

        # การ์ดรายวัน
        cols = st.columns(len(daily_counts))
        for i, row in daily_counts.iterrows():
            day = row["Date"]
            count = row["Sites"]
            with cols[i]:
                st.metric(label=str(day), value=f"{count} sites")
                if st.button("Show Details", key=f"btn_{day}"):
                    st.session_state["selected_day"] = day

        # Drill-down ตาราง
        if st.session_state["selected_day"]:
            sel_day = st.session_state["selected_day"]
            sel = df_nomatch[df_nomatch["Date"] == sel_day]

            st.markdown(f"#### Details for {sel_day}")
            if sel.empty:
                st.info("No flapping records on this day")
            else:
                # ✅ เลือกเฉพาะคอลัมน์ที่ต้องการ (ไม่มี Target ME, Date)
                view_cols = [
                    "Begin Time", "End Time", "ME", "Measure Object",
                    "Max Value of Input Optical Power(dBm)",
                    "Min Value of Input Optical Power(dBm)",
                    "Input Optical Power(dBm)", "Max - Min (dB)"
                ]
                view_cols = [c for c in view_cols if c in sel.columns]
                sel = sel[view_cols]

                # ✅ ทำ highlight คอลัมน์ Max - Min (dB)
                if "Max - Min (dB)" in sel.columns:
                    styled_sel = (
                        sel.style
                        .apply(
                            lambda _:
                                ['background-color:#ff4d4d; color:white' if (v > self.threshold) else ''
                                for v in sel["Max - Min (dB)"]],
                            subset=["Max - Min (dB)"]
                        )
                        .format({
                            "Max Value of Input Optical Power(dBm)": "{:.2f}",
                            "Min Value of Input Optical Power(dBm)": "{:.2f}",
                            "Input Optical Power(dBm)": "{:.2f}",
                            "Max - Min (dB)": "{:.2f}",
                        })
                    )
                    st.write(styled_sel)
                else:
                    st.dataframe(sel, use_container_width=True)
                    

        # 📊 กราฟรวม (ท้ายสุด)
        if not daily_counts.empty:
            fig = px.bar(daily_counts, x="Date", y="Sites", text="Sites", title="No Fiber Break Alarm Match(Fiber Flapping)")
            fig.update_traces(textposition="outside")
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    # -------------------- Export helper (NEW) --------------------
    @staticmethod
    def _select_view_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        เลือกคอลัมน์ให้เหมือน drill-down แล้วจัดรูปตัวเลข (ใช้สำหรับ export/report)
        """
        view_cols = [
            "Begin Time", "End Time", "ME", "Measure Object",
            "Max Value of Input Optical Power(dBm)",
            "Min Value of Input Optical Power(dBm)",
            "Input Optical Power(dBm)", "Max - Min (dB)"
        ]
        have = [c for c in view_cols if c in df.columns]
        out = df[have].copy()

        num_cols = [c for c in [
            "Max Value of Input Optical Power(dBm)",
            "Min Value of Input Optical Power(dBm)",
            "Input Optical Power(dBm)", "Max - Min (dB)"
        ] if c in out.columns]
        if num_cols:
            out.loc[:, num_cols] = out[num_cols].apply(pd.to_numeric, errors="coerce").round(2)
        return out

    def build_daily_tables(self, df_nomatch: pd.DataFrame) -> "OrderedDict[str, pd.DataFrame]":
        """
        สร้าง dict รายวัน -> DataFrame (คอลัมน์เหมือน drill-down) สำหรับ export
        เช่น {"2025-06-17": df_table, "2025-06-18": df_table, ...}
        """
        if df_nomatch.empty:
            self.daily_tables = OrderedDict()
            return self.daily_tables

        df = df_nomatch.copy()
        df["Date"] = pd.to_datetime(df["Begin Time"]).dt.date

        tables = OrderedDict()
        for day, g in df.sort_values("Begin Time").groupby("Date", sort=True):
            tables[str(day)] = self._select_view_columns(g)
        self.daily_tables = tables
        return tables

    # -------------------- Orchestration --------------------
    def process(self) -> None:
        # 1) เตรียมข้อมูล
        df_optical_norm = self.normalize_optical()
        df_fm_norm, link_col = self.normalize_fm()

        # 2) กรองตาม threshold
        df_filtered = self.filter_optical_by_threshold(df_optical_norm)

        # 3) หา no-match
        df_nomatch = self.find_nomatch(df_filtered, df_fm_norm, link_col)

        # 4) ตารางหลัก
        self.render(df_nomatch)

        # 5) Weekly Summary KPI + กราฟท้ายสุด
        self.render_weekly_summary(df_nomatch)

    
