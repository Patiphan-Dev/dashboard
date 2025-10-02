import pandas as pd
import streamlit as st
from utils.filters import cascading_filter
import plotly.graph_objects as go


# ต้องมีฟังก์ชันนี้ให้เรียกใช้งานได้
# def cascading_filter(df, cols, ns, labels=None, clear_text="Clear Filters"): ...


class Client_Analyzer:
    """
    จัดระเบียบโค้ด 'Client board' เดิมให้อยู่ในคลาสเดียว โดยคงตรรกะ/พฤติกรรมเดิมทั้งหมด:
      - โหลด/ทำความสะอาดคอลัมน์
      - ตรวจคอลัมน์ที่จำเป็น
      - สร้าง Mapping Format, โหลด reference, merge ด้วย Mapping
      - เรียง order ตาม ref
      - ใช้ cascading_filter
      - ไฮไลท์: เทาทั้งแถวที่มีปัญหา + แดงเฉพาะค่าที่ผิด
      - แสดงแบนเนอร์สถานะ (Warning/Normal)

    วิธีใช้:
        analyzer = Client_Analyzer(df_client, ref_path="data/Client.xlsx")
        analyzer.process()
    """

    REQ_CLIENT_COLS = {"ME", "Measure Object", "Input Optical Power(dBm)", "Output Optical Power (dBm)"}
    REQ_REF_COLS = {
        "Mapping",
        "Maximum threshold(out)",
        "Minimum threshold(out)",
        "Maximum threshold(in)",
        "Minimum threshold(in)"
    }

    # ชื่อคอลัมน์สำคัญ (ยึดตามต้นฉบับ)
    COL_OUT = "Output Optical Power (dBm)"
    COL_IN = "Input Optical Power(dBm)"
    COL_MAX_OUT = "Maximum threshold(out)"
    COL_MIN_OUT = "Minimum threshold(out)"
    COL_MAX_IN = "Maximum threshold(in)"
    COL_MIN_IN = "Minimum threshold(in)"

    def __init__(self, df_client: pd.DataFrame, ref_path: str = "data/Client.xlsx"):
        self.df_client_raw = df_client
        self.ref_path = ref_path


        # สถานะระหว่างทาง
        self.df_client = None
        self.df_ref = None
        self.df_merged = None
        self.df_result = None
        self.df_filtered = None

        self.df_abnormal = pd.DataFrame()
        self.df_abnormal_by_type = {}

    # -------------------- Step 1: Normalize & Validate --------------------
    @staticmethod
    def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.copy()
            .rename(columns=lambda c: str(c))
            .pipe(lambda _df: _df.set_axis(
                _df.columns.astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace("\u00a0", " "), axis=1
            ))
        )

    @staticmethod
    def _normalize_ref_cols(df: pd.DataFrame) -> pd.DataFrame:
        # ตรงกับตรรกะเดิม: encode('ascii','ignore') → decode
        df2 = df.copy()
        df2.columns = (
            df2.columns.astype(str)
            .str.encode("ascii", "ignore").str.decode("utf-8")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        return df2

    def _validate_client_cols(self, df: pd.DataFrame):
        if not self.REQ_CLIENT_COLS.issubset(df.columns):
            st.error(f"Client file must contain columns: {', '.join(self.REQ_CLIENT_COLS)}")
            st.stop()

    def _validate_ref_cols(self, df: pd.DataFrame):
        if not self.REQ_REF_COLS.issubset(df.columns):
            st.error(f"Reference file must contain columns: {', '.join(self.REQ_REF_COLS)}")
            st.stop()

    # -------------------- Step 2: Build Mapping & Load Reference --------------------
    def _build_mapping_format(self, df: pd.DataFrame) -> pd.DataFrame:
        df2 = df.copy()
        df2["Mapping Format"] = df2["ME"].astype(str).str.strip() + df2["Measure Object"].astype(str).str.strip()
        return df2

    def _load_reference(self) -> pd.DataFrame:
        ref = pd.read_excel(self.ref_path)
        ref = self._normalize_ref_cols(ref)
        self._validate_ref_cols(ref)
        ref["Mapping"] = ref["Mapping"].astype(str).str.strip()
        ref["order"] = range(len(ref))  # รักษาลำดับตามไฟล์ reference
        return ref

    # -------------------- Step 3: Merge & Prepare View --------------------
    def _merge(self, df_client: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
        df_merged = pd.merge(
            df_client,
            df_ref[[
                "Site Name", "Mapping",
                self.COL_MAX_OUT, self.COL_MIN_OUT,
                self.COL_MAX_IN, self.COL_MIN_IN,
                "order"
            ]],
            left_on="Mapping Format", right_on="Mapping", how="inner",
        )
        return df_merged

    @staticmethod
    def _numeric_cast(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        df2 = df.copy()
        for c in cols:
            if c in df2.columns:
                df2.loc[:, c] = pd.to_numeric(df2[c], errors="coerce")
        return df2

    # -------------------- Step 4: Filter UI (cascading_filter) --------------------
    def _apply_cascading_filter(self, df: pd.DataFrame):
        df_filtered, _sel = cascading_filter(
            df,
            cols=["Site Name", "ME", "Measure Object"],
            ns="client",
            clear_text="Clear Client Filters",
        )
        st.caption(f"Client (showing {len(df_filtered)}/{len(df)} rows)")
        return df_filtered

    # -------------------- Step 5: Styling --------------------
    def _row_has_issue(self, r: pd.Series) -> bool:
        try:
            return (
                (r[self.COL_OUT] > r[self.COL_MAX_OUT]) or (r[self.COL_OUT] < r[self.COL_MIN_OUT]) or
                (r[self.COL_IN] > r[self.COL_MAX_IN]) or (r[self.COL_IN] < r[self.COL_MIN_IN])
            )
        except Exception:
            return False

    def _highlight_critical_cells(self, val, colname, row) -> str:
        try:
            if colname == self.COL_OUT:
                return 'background-color:#ff4d4d; color:white' if (val > row[self.COL_MAX_OUT] or val < row[self.COL_MIN_OUT]) else ''
            elif colname == self.COL_IN:
                return 'background-color:#ff4d4d; color:white' if (val > row[self.COL_MAX_IN] or val < row[self.COL_MIN_IN]) else ''
            return ''
        except Exception:
            return ''

    def _style_dataframe(self, df_view: pd.DataFrame):
        styled_df = (
            df_view.style
            # เทาทั้งแถวเมื่อมีปัญหา
            .apply(lambda r: ['background-color:#e6e6e6; color:black' if self._row_has_issue(r) else '' for _ in r], axis=1)
            # แดงเฉพาะค่าที่ผิด (ทั้ง out/in)
            .apply(lambda row: [self._highlight_critical_cells(row[colname], colname, row) for colname in df_view.columns], axis=1)
            .format({
                self.COL_MAX_OUT: "{:.2f}",
                self.COL_MIN_OUT: "{:.2f}",
                self.COL_MAX_IN: "{:.2f}",
                self.COL_MIN_IN: "{:.2f}",
                self.COL_OUT: "{:.2f}",
                self.COL_IN: "{:.2f}",
            })
        )
        return styled_df

    # -------------------- Step 6: Banner --------------------
    def _render_status_banner(self, df_view: pd.DataFrame):
        failed_rows = df_view.apply(self._row_has_issue, axis=1)
        st.markdown(
            "<div style='text-align:center; font-size:32px; font-weight:bold; color:{};'>Client Performance {}</div>".format(
                "red" if failed_rows.any() else "green",
                "Warning" if failed_rows.any() else "Normal"
            ),
            unsafe_allow_html=True
        )
        self._render_summary_kpi(self.df_filtered)
        self._render_c2k_avg_slot_charts(df_view)
        self._render_c2l_avg_slot_charts(df_view)
        self._render_c4r_avg_slot_charts(df_view)

    # -------------------- VISUALIZATION --------------------
    def process(self):
        # 1) ทำความสะอาด & ตรวจคอลัมน์ client
        self.df_client = self._normalize_cols(self.df_client_raw)
        self._validate_client_cols(self.df_client)

        # 2) สร้าง Mapping Format
        self.df_client = self._build_mapping_format(self.df_client)

        # 3) โหลด reference & merge
        self.df_ref = self._load_reference()
        self.df_merged = self._merge(self.df_client, self.df_ref)

        if self.df_merged.empty:
            st.warning("No matching mapping found between Client file and reference")
            return

        # 4) เรียงตาม order จาก ref + จัดคอลัมน์แสดงผล
        self.df_merged = self.df_merged.sort_values("order").reset_index(drop=True)
        self.df_result = self.df_merged[[
            "Site Name", "ME", "Measure Object",
            self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_OUT,
            self.COL_MAX_IN, self.COL_MIN_IN, self.COL_IN
        ]].copy()

        # 5) แปลงเป็นตัวเลขก่อนเทียบ
        self.df_result = self._numeric_cast(
            self.df_result,
            [self.COL_OUT, self.COL_IN, self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_MAX_IN, self.COL_MIN_IN]
        )

        # 6) Cascading filter
        self.df_filtered = self._apply_cascading_filter(self.df_result)

        # 7) เรนเดอร์ตาราง + แบนเนอร์
        st.markdown("### Client Performance")
        styled_df = self._style_dataframe(self.df_filtered.copy())
        st.dataframe(styled_df, use_container_width=True)
        self._render_status_banner(self.df_filtered)

    def _render_summary_kpi(self, df_view: pd.DataFrame) -> None:
        """Summary KPI: Client Links (unique) vs Threshold"""
        st.markdown("### Overall Client Performance")

        # เตรียมข้อมูล
        df = df_view.copy()
        # กรองทิ้ง -60
        df[self.COL_IN] = pd.to_numeric(df.get(self.COL_IN), errors="coerce")
        df[self.COL_OUT] = pd.to_numeric(df.get(self.COL_OUT), errors="coerce")
        df[self.COL_MIN_IN] = pd.to_numeric(df.get(self.COL_MIN_IN), errors="coerce")
        df[self.COL_MAX_IN] = pd.to_numeric(df.get(self.COL_MAX_IN), errors="coerce")
        df[self.COL_MIN_OUT] = pd.to_numeric(df.get(self.COL_MIN_OUT), errors="coerce")
        df[self.COL_MAX_OUT] = pd.to_numeric(df.get(self.COL_MAX_OUT), errors="coerce")
        df = df[(df[self.COL_IN] != -60) & (df[self.COL_OUT] != -60)]

        if df.empty:
            st.info("No valid Client Link data (after filtering -60).")
            return

        # คำนวณ OK/Fail ราย row
        df["in_ok"] = (
            df[self.COL_IN].notna()
            & (df[self.COL_IN] >= df[self.COL_MIN_IN])
            & (df[self.COL_IN] <= df[self.COL_MAX_IN])
        )
        df["out_ok"] = (
            df[self.COL_OUT].notna()
            & (df[self.COL_OUT] >= df[self.COL_MIN_OUT])
            & (df[self.COL_OUT] <= df[self.COL_MAX_OUT])
        )

        # รวมเป็นราย Link (Site + Measure Object)
        link_status = (
            df.groupby(["Site Name", "Measure Object"])
            .agg(link_ok=("in_ok", "all"))  # เริ่มจาก input
            .reset_index()
        )

        # รวม input และ output -> ลิงก์จะ OK ก็ต่อเมื่อ in_ok และ out_ok ทั้งคู่ true
        link_status = (
            df.groupby(["Site Name", "Measure Object"])
            .agg(link_ok=("in_ok", "all"))
            .merge(
                df.groupby(["Site Name", "Measure Object"]).agg(out_ok=("out_ok", "all")),
                on=["Site Name", "Measure Object"],
                how="left",
            )
        )
        link_status["link_ok"] = link_status["link_ok"] & link_status["out_ok"]

        # สรุป
        total_links = len(link_status)
        ok_links = int(link_status["link_ok"].sum())
        fail_links = total_links - ok_links

        # แสดง KPI
        cols = st.columns(3)
        cols[0].metric("Client Links OK", f"{ok_links}")
        cols[1].metric("Client Links Fail", f"{fail_links}")
        cols[2].metric("Total Client Links", f"{total_links}")
        st.markdown("<br><br>", unsafe_allow_html=True)

    # =====================================================================
    # C2K
    # =====================================================================
    def _render_c2k_avg_slot_charts(self, df_view: pd.DataFrame) -> None:
        """C2K: Average Input/Output Power per Slot (lines+markers)"""
        st.markdown("### C2K Board Performance (Avg per Slot)")

        # ---------------- Filter เฉพาะ C2K ----------------
        df_c2k = df_view[df_view["Measure Object"].astype(str).str.startswith("C2K", na=False)].copy()
        if df_c2k.empty:
            st.info("No C2K rows found.")
            return

        vin_raw = pd.to_numeric(df_c2k.get(self.COL_IN), errors="coerce")
        vout_raw = pd.to_numeric(df_c2k.get(self.COL_OUT), errors="coerce")
        mask_io = vin_raw.notna() | vout_raw.notna()
        mask_valid = (vin_raw != -60) & (vout_raw != -60)  # filter ทิ้ง input/output = -60
        df_c2k = df_c2k.loc[mask_io & mask_valid].copy()
        if df_c2k.empty:
            st.info("No C2K rows with valid Input/Output values.")
            return

        # ---------------- Extract Slot ----------------
        slot = df_c2k["Measure Object"].str.extract(r"^(C2Kx\d+\[[^\]]+\])")[0]
        slot = slot.fillna(df_c2k["Measure Object"].str.extract(r"^(C2K[^\-\s]+)")[0])
        df_c2k["Board Slot"] = slot
        for c in [
            self.COL_IN, self.COL_OUT,
            self.COL_MIN_IN, self.COL_MAX_IN,
            self.COL_MIN_OUT, self.COL_MAX_OUT
        ]:
            if c in df_c2k.columns:
                df_c2k[c] = pd.to_numeric(df_c2k[c], errors="coerce")

        order_pairs = df_c2k.drop_duplicates(subset=["Site Name", "Board Slot"]).reset_index(drop=True)
        order_pairs["_ord"] = range(len(order_pairs))

        # ---------------- Aggregate per Slot ----------------
        agg = (
            df_c2k.groupby(["Site Name", "Board Slot"])
            .agg(
                avg_in=(self.COL_IN, "mean"),
                avg_out=(self.COL_OUT, "mean"),
                min_in=(self.COL_MIN_IN, "first"),
                max_in=(self.COL_MAX_IN, "first"),
                min_out=(self.COL_MIN_OUT, "first"),
                max_out=(self.COL_MAX_OUT, "first"),
            )
            .reset_index()
            .merge(order_pairs[["Site Name", "Board Slot", "_ord"]],
                   on=["Site Name", "Board Slot"], how="left")
            .sort_values("_ord", kind="stable")
            .reset_index(drop=True)
        )
        if agg.empty:
            st.info("No aggregated C2K slots to display.")
            return

        labels = agg["Site Name"].astype(str) + " • " + agg["Board Slot"].astype(str)
        x_index = list(range(len(agg)))

        # ---------------- Check abnormal ----------------
        df_c2k["row_abnormal_in"] = (
            df_c2k[self.COL_IN].notna()
            & ((df_c2k[self.COL_IN] < df_c2k[self.COL_MIN_IN]) | (df_c2k[self.COL_IN] > df_c2k[self.COL_MAX_IN]))
        )
        df_c2k["row_abnormal_out"] = (
            df_c2k[self.COL_OUT].notna()
            & ((df_c2k[self.COL_OUT] < df_c2k[self.COL_MIN_OUT]) | (df_c2k[self.COL_OUT] > df_c2k[self.COL_MAX_OUT]))
        )
        slot_abnormal_in = (
            df_c2k.groupby(["Site Name", "Board Slot"])["row_abnormal_in"]
            .any().reset_index().rename(columns={"row_abnormal_in": "slot_abnormal_in"})
        )
        slot_abnormal_out = (
            df_c2k.groupby(["Site Name", "Board Slot"])["row_abnormal_out"]
            .any().reset_index().rename(columns={"row_abnormal_out": "slot_abnormal_out"})
        )
        agg = agg.merge(slot_abnormal_in, on=["Site Name", "Board Slot"], how="left")
        agg = agg.merge(slot_abnormal_out, on=["Site Name", "Board Slot"], how="left")

        colors_in = ["red" if flag else "orange" for flag in agg["slot_abnormal_in"]]
        colors_out = ["red" if flag else "blue" for flag in agg["slot_abnormal_out"]]

        # ---------------- Plot ----------------
        fig = go.Figure()
        unique_in = agg[["min_in", "max_in"]].dropna().drop_duplicates()
        unique_out = agg[["min_out", "max_out"]].dropna().drop_duplicates()
        if len(unique_in) == 1:
            fig.add_hrect(y0=float(unique_in.iloc[0]["min_in"]), y1=float(unique_in.iloc[0]["max_in"]),
                          fillcolor="orange", opacity=0.10, line_width=0)
        if len(unique_out) == 1:
            fig.add_hrect(y0=float(unique_out.iloc[0]["min_out"]), y1=float(unique_out.iloc[0]["max_out"]),
                          fillcolor="blue", opacity=0.10, line_width=0)

        fig.add_trace(go.Scatter(
            x=x_index, y=agg["avg_in"], mode="lines+markers+text",
            marker=dict(color=colors_in, size=8, symbol="circle"), line=dict(color="orange"),
            name="Input Power (avg)",
            text=[f"{v:.2f} dBm" for v in agg["avg_in"]],
            textposition="bottom center", textfont=dict(color="orange", size=12),
        ))
        fig.add_trace(go.Scatter(
            x=x_index, y=agg["avg_out"], mode="lines+markers+text",
            marker=dict(color=colors_out, size=8, symbol="square"), line=dict(color="blue"),
            name="Output Power (avg)",
            text=[f"{v:.2f} dBm" for v in agg["avg_out"]],
            textposition="top center", textfont=dict(color="blue", size=12),
        ))

        fig.update_layout(
            title="C2K Avg per Slot (Threshold: Input -16.40 ~ +2.50 dBm, Output -10.99 ~ +0.99 dBm)",
            yaxis_title="Optical Power (dBm)",
            xaxis=dict(
                title="Site • Slot",
                tickmode="array", tickvals=x_index, ticktext=labels,
                tickangle=45, automargin=True
            ),
            legend=dict(orientation="h", y=1.12, x=1, xanchor="right"),
            height=600, margin=dict(b=160, l=40, r=40, t=40),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ---------------- Show abnormal table ----------------
        df_c2k_probs = df_c2k[df_c2k["row_abnormal_in"] | df_c2k["row_abnormal_out"]].copy()
        if not df_c2k_probs.empty:
            st.markdown(" Abnormal C2K rows ")
            cols_show = [
                "Site Name", "ME", "Measure Object",
                self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_OUT,
                self.COL_MAX_IN, self.COL_MIN_IN, self.COL_IN
            ]

            # ✅ ใช้ style ให้เน้นแดงเฉพาะค่าที่ผิด
            styled_abn = (
                df_c2k_probs[cols_show].style
                .apply(
                    lambda row: [self._highlight_critical_cells(row[col], col, row) for col in df_c2k_probs[cols_show].columns],
                    axis=1
                )
                .format("{:.2f}", subset=[
                    self.COL_OUT, self.COL_IN,
                    self.COL_MAX_OUT, self.COL_MIN_OUT,
                    self.COL_MAX_IN, self.COL_MIN_IN
                ])
            )

            st.dataframe(styled_abn, use_container_width=True)
            self.df_c2k_abn = df_c2k_probs[cols_show].copy()
        else:
            st.success("All C2K rows are within threshold.")
            self.df_c2k_abn = None


    # =====================================================================
    # C2L
    # =====================================================================
    def _render_c2l_avg_slot_charts(self, df_view: pd.DataFrame) -> None:
        """C2L: Average Input/Output Power per Slot (lines+markers)"""
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("### C2L Board Performance (Avg per Slot)")

        df_c2l = df_view[df_view["Measure Object"].astype(str).str.startswith("C2L", na=False)].copy()
        if df_c2l.empty:
            st.info("No C2L rows found.")
            return

        vin_raw = pd.to_numeric(df_c2l.get(self.COL_IN), errors="coerce")
        vout_raw = pd.to_numeric(df_c2l.get(self.COL_OUT), errors="coerce")
        mask_io = vin_raw.notna() | vout_raw.notna()
        mask_valid = (vin_raw != -60) & (vout_raw != -60)
        df_c2l = df_c2l.loc[mask_io & mask_valid].copy()
        if df_c2l.empty:
            st.info("No C2L rows with valid Input/Output values.")
            return

        slot = df_c2l["Measure Object"].str.extract(r"^(C2Lx\d+\[[^\]]+\])")[0]
        slot = slot.fillna(df_c2l["Measure Object"].str.extract(r"^(C2L[^\-\s]+)")[0])
        df_c2l["Board Slot"] = slot
        for c in [
            self.COL_IN, self.COL_OUT,
            self.COL_MIN_IN, self.COL_MAX_IN,
            self.COL_MIN_OUT, self.COL_MAX_OUT
        ]:
            if c in df_c2l.columns:
                df_c2l[c] = pd.to_numeric(df_c2l[c], errors="coerce")

        order_pairs = df_c2l.drop_duplicates(subset=["Site Name", "Board Slot"]).reset_index(drop=True)
        order_pairs["_ord"] = range(len(order_pairs))

        agg = (
            df_c2l.groupby(["Site Name", "Board Slot"])
            .agg(
                avg_in=(self.COL_IN, "mean"),
                avg_out=(self.COL_OUT, "mean"),
                min_in=(self.COL_MIN_IN, "first"),
                max_in=(self.COL_MAX_IN, "first"),
                min_out=(self.COL_MIN_OUT, "first"),
                max_out=(self.COL_MAX_OUT, "first"),
            )
            .reset_index()
            .merge(order_pairs[["Site Name", "Board Slot", "_ord"]],
                   on=["Site Name", "Board Slot"], how="left")
            .sort_values("_ord", kind="stable")
            .reset_index(drop=True)
        )
        if agg.empty:
            st.info("No aggregated C2L slots to display.")
            return

        labels = agg["Site Name"].astype(str) + " • " + agg["Board Slot"].astype(str)
        x_index = list(range(len(agg)))

        # ---------------- Check abnormal ----------------
        df_c2l["row_abnormal_in"] = (
            df_c2l[self.COL_IN].notna()
            & ((df_c2l[self.COL_IN] < df_c2l[self.COL_MIN_IN]) | (df_c2l[self.COL_IN] > df_c2l[self.COL_MAX_IN]))
        )
        df_c2l["row_abnormal_out"] = (
            df_c2l[self.COL_OUT].notna()
            & ((df_c2l[self.COL_OUT] < df_c2l[self.COL_MIN_OUT]) | (df_c2l[self.COL_OUT] > df_c2l[self.COL_MAX_OUT]))
        )
        slot_abnormal_in = (
            df_c2l.groupby(["Site Name", "Board Slot"])["row_abnormal_in"]
            .any().reset_index().rename(columns={"row_abnormal_in": "slot_abnormal_in"})
        )
        slot_abnormal_out = (
            df_c2l.groupby(["Site Name", "Board Slot"])["row_abnormal_out"]
            .any().reset_index().rename(columns={"row_abnormal_out": "slot_abnormal_out"})
        )
        agg = agg.merge(slot_abnormal_in, on=["Site Name", "Board Slot"], how="left")
        agg = agg.merge(slot_abnormal_out, on=["Site Name", "Board Slot"], how="left")

        colors_in = ["red" if flag else "orange" for flag in agg["slot_abnormal_in"]]
        colors_out = ["red" if flag else "blue" for flag in agg["slot_abnormal_out"]]

        # ---------------- Plot ----------------
        fig = go.Figure()
        unique_in = agg[["min_in", "max_in"]].dropna().drop_duplicates()
        unique_out = agg[["min_out", "max_out"]].dropna().drop_duplicates()
        if len(unique_in) == 1:
            fig.add_hrect(y0=float(unique_in.iloc[0]["min_in"]), y1=float(unique_in.iloc[0]["max_in"]),
                          fillcolor="orange", opacity=0.10, line_width=0)
        if len(unique_out) == 1:
            fig.add_hrect(y0=float(unique_out.iloc[0]["min_out"]), y1=float(unique_out.iloc[0]["max_out"]),
                          fillcolor="blue", opacity=0.10, line_width=0)

        fig.add_trace(go.Scatter(
            x=x_index, y=agg["avg_in"], mode="lines+markers+text",
            marker=dict(color=colors_in, size=8, symbol="circle"), line=dict(color="orange"),
            name="Input Power (avg)",
            text=[f"{v:.2f} dBm" for v in agg["avg_in"]],
            textposition="bottom center", textfont=dict(color="orange", size=12),
        ))
        fig.add_trace(go.Scatter(
            x=x_index, y=agg["avg_out"], mode="lines+markers+text",
            marker=dict(color=colors_out, size=8, symbol="square"), line=dict(color="blue"),
            name="Output Power (avg)",
            text=[f"{v:.2f} dBm" for v in agg["avg_out"]],
            textposition="top center", textfont=dict(color="blue", size=12),
        ))

        fig.update_layout(
            title="C2L Avg per Slot (Threshold: Input -16.40 ~ +2.50 dBm, Output -10.99 ~ +0.99 dBm)",
            yaxis_title="Optical Power (dBm)",
            xaxis=dict(
                title="Site • Slot",
                tickmode="array", tickvals=x_index, ticktext=labels,
                tickangle=45, automargin=True
            ),
            legend=dict(orientation="h", y=1.12, x=1, xanchor="right"),
            height=600, margin=dict(b=160, l=40, r=40, t=40),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ---------------- Show abnormal table ----------------
        df_c2l_probs = df_c2l[df_c2l["row_abnormal_in"] | df_c2l["row_abnormal_out"]].copy()
        if not df_c2l_probs.empty:
            st.markdown(" Abnormal C2L rows ")
            cols_show = [
                "Site Name", "ME", "Measure Object",
                self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_OUT,
                self.COL_MAX_IN, self.COL_MIN_IN, self.COL_IN
            ]

            styled_abn = (
                df_c2l_probs[cols_show].style
                .apply(lambda row: [
                    self._highlight_critical_cells(row[col], col, row)
                    for col in df_c2l_probs[cols_show].columns
                ], axis=1)
                .format("{:.2f}", subset=[
                    self.COL_OUT, self.COL_IN,
                    self.COL_MAX_OUT, self.COL_MIN_OUT,
                    self.COL_MAX_IN, self.COL_MIN_IN
                ])
            )
            st.dataframe(styled_abn, use_container_width=True)
            self.df_c2l_abn = df_c2l_probs[cols_show].copy()
        else:
            st.success("All C2L rows are within threshold.")
            self.df_c2l_abn = None

    # =====================================================================
    # C4R
    # =====================================================================
    def _render_c4r_avg_slot_charts(self, df_view: pd.DataFrame) -> None:
        """C4R: Avg per Slot + จุดเป็นสีแดงถ้า (ลิงก์ใดผิด) หรือ (avg เกิน main threshold)"""
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("### C4R Board Performance (Avg per Slot)")

        # --- Filter เฉพาะ C4R ---
        df_c4r = df_view[df_view["Measure Object"].astype(str).str.startswith("C4R", na=False)].copy()
        if df_c4r.empty:
            st.info("No C4R rows found.")
            return

        # แปลงเลขทุกคอลัมน์ที่จำเป็น
        for c in [
            self.COL_IN, self.COL_OUT,
            self.COL_MIN_IN, self.COL_MAX_IN,
            self.COL_MIN_OUT, self.COL_MAX_OUT
        ]:
            if c in df_c4r.columns:
                df_c4r[c] = pd.to_numeric(df_c4r[c], errors="coerce")

        vin_raw = df_c4r[self.COL_IN]
        vout_raw = df_c4r[self.COL_OUT]

        # กรองค่า -60 (invalid) และ keep เฉพาะแถวที่มีค่า in/out อย่างน้อยหนึ่งด้าน
        mask_io = vin_raw.notna() | vout_raw.notna()
        mask_valid = (vin_raw != -60) & (vout_raw != -60)
        df_c4r = df_c4r.loc[mask_io & mask_valid].copy()
        if df_c4r.empty:
            st.info("No C4R rows with valid Input/Output values.")
            return

        # --- Extract slot ---
        slot = df_c4r["Measure Object"].str.extract(r"^(C4Rx\d+\[[^\]]+\])")[0]
        slot = slot.fillna(df_c4r["Measure Object"].str.extract(r"^(C4R[^\-\s]+)")[0])
        df_c4r["Board Slot"] = slot

        # รักษา order ของ Site x Slot ให้ stable
        order_pairs = df_c4r.drop_duplicates(subset=["Site Name", "Board Slot"]).reset_index(drop=True)
        order_pairs["_ord"] = range(len(order_pairs))

        # --- Aggregate avg per slot ---
        agg = (
            df_c4r.groupby(["Site Name", "Board Slot"], as_index=False)
            .agg(
                avg_in=(self.COL_IN, "mean"),
                avg_out=(self.COL_OUT, "mean"),
                # เก็บ threshold ของแถวแรกไว้ใช้อ้างอิง/แสดง (ไม่ใช้ตัดสิน avg)
                min_in=(self.COL_MIN_IN, "first"),
                max_in=(self.COL_MAX_IN, "first"),
                min_out=(self.COL_MIN_OUT, "first"),
                max_out=(self.COL_MAX_OUT, "first"),
            )
            .merge(order_pairs[["Site Name", "Board Slot", "_ord"]],
                   on=["Site Name", "Board Slot"], how="left")
            .sort_values("_ord", kind="stable")
            .reset_index(drop=True)
        )
        if agg.empty:
            st.info("No aggregated C4R slots to display.")
            return

        # --- Main threshold (สำหรับกราฟเท่านั้น) ---
        MAIN_MIN_IN, MAIN_MAX_IN = -6.57, 11.52
        MAIN_MIN_OUT, MAIN_MAX_OUT = -0.27, 11.52

        # --- Abnormal จากค่าดิบรายลิงก์ (per-row) แยก In/Out ---
        df_c4r["row_abnormal_in"] = (
            df_c4r[self.COL_IN].notna() & df_c4r[self.COL_MIN_IN].notna() & df_c4r[self.COL_MAX_IN].notna()
            & ((df_c4r[self.COL_IN] < df_c4r[self.COL_MIN_IN]) | (df_c4r[self.COL_IN] > df_c4r[self.COL_MAX_IN]))
        )
        df_c4r["row_abnormal_out"] = (
            df_c4r[self.COL_OUT].notna() & df_c4r[self.COL_MIN_OUT].notna() & df_c4r[self.COL_MAX_OUT].notna()
            & ((df_c4r[self.COL_OUT] < df_c4r[self.COL_MIN_OUT]) | (df_c4r[self.COL_OUT] > df_c4r[self.COL_MAX_OUT]))
        )

        slot_abnormal_in = (
            df_c4r.groupby(["Site Name", "Board Slot"], as_index=False)["row_abnormal_in"]
            .any().rename(columns={"row_abnormal_in": "slot_abnormal_in"})
        )
        slot_abnormal_out = (
            df_c4r.groupby(["Site Name", "Board Slot"], as_index=False)["row_abnormal_out"]
            .any().rename(columns={"row_abnormal_out": "slot_abnormal_out"})
        )

        # --- Abnormal จากค่าเฉลี่ย (เทียบ main threshold) แยก In/Out ---
        agg["avg_abnormal_in"] = (
            agg["avg_in"].notna() & ((agg["avg_in"] < MAIN_MIN_IN) | (agg["avg_in"] > MAIN_MAX_IN))
        )
        agg["avg_abnormal_out"] = (
            agg["avg_out"].notna() & ((agg["avg_out"] < MAIN_MIN_OUT) | (agg["avg_out"] > MAIN_MAX_OUT))
        )

        # --- รวมธง abnormal แยก In/Out ---
        agg = agg.merge(slot_abnormal_in, on=["Site Name", "Board Slot"], how="left")
        agg = agg.merge(slot_abnormal_out, on=["Site Name", "Board Slot"], how="left")
        agg["slot_abnormal_in"] = agg["slot_abnormal_in"].fillna(False)
        agg["slot_abnormal_out"] = agg["slot_abnormal_out"].fillna(False)
        agg["is_abnormal_in"] = agg["slot_abnormal_in"] | agg["avg_abnormal_in"]
        agg["is_abnormal_out"] = agg["slot_abnormal_out"] | agg["avg_abnormal_out"]

        # --- เตรียมแกนและสี ---
        labels = agg["Site Name"].astype(str) + " • " + agg["Board Slot"].astype(str)
        x_index = list(range(len(agg)))
        colors_in = ["red" if flag else "orange" for flag in agg["is_abnormal_in"]]
        colors_out = ["red" if flag else "blue" for flag in agg["is_abnormal_out"]]

        # --- วาดกราฟ ---
        fig = go.Figure()
        fig.add_hrect(y0=MAIN_MIN_IN, y1=MAIN_MAX_IN, fillcolor="orange", opacity=0.10, line_width=0)
        fig.add_hrect(y0=MAIN_MIN_OUT, y1=MAIN_MAX_OUT, fillcolor="blue", opacity=0.10, line_width=0)

        fig.add_trace(go.Scatter(
            x=x_index, y=agg["avg_in"], mode="lines+markers+text",
            marker=dict(color=colors_in, size=8, symbol="circle"), line=dict(color="orange"),
            name="Input Power (avg)",
            text=[f"{v:.2f} dBm" if pd.notna(v) else "" for v in agg["avg_in"]],
            textposition="bottom center", textfont=dict(color="orange", size=12),
        ))
        fig.add_trace(go.Scatter(
            x=x_index, y=agg["avg_out"], mode="lines+markers+text",
            marker=dict(color=colors_out, size=8, symbol="square"), line=dict(color="blue"),
            name="Output Power (avg)",
            text=[f"{v:.2f} dBm" if pd.notna(v) else "" for v in agg["avg_out"]],
            textposition="top center", textfont=dict(color="blue", size=12),
        ))

        fig.update_layout(
            title="C4R Avg per Slot (Threshold: Input -6.57 ~ +11.52 dBm, Output -0.27 ~ +11.52 dBm)",
            yaxis_title="Optical Power (dBm)",
            xaxis=dict(
                title="Site • Slot",
                tickmode="array", tickvals=x_index, ticktext=labels,
                tickangle=45, automargin=True
            ),
            legend=dict(orientation="h", y=1.12, x=1, xanchor="right"),
            height=600, margin=dict(b=160, l=40, r=40, t=40),
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- ตาราง Abnormal (รายลิงก์) ตาม threshold ของแถวตัวเอง ---
        df_c4r_probs = df_c4r.loc[df_c4r["row_abnormal_in"] | df_c4r["row_abnormal_out"]].copy()
        if not df_c4r_probs.empty:
            st.markdown(" Abnormal C4R rows ")
            cols_show = [
                "Site Name", "ME", "Measure Object",
                self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_OUT,
                self.COL_MAX_IN, self.COL_MIN_IN, self.COL_IN
            ]

            styled_abn = (
                df_c4r_probs[cols_show].style
                .apply(lambda row: [
                    self._highlight_critical_cells(row[col], col, row)
                    for col in df_c4r_probs[cols_show].columns
                ], axis=1)
                .format("{:.2f}", subset=[
                    self.COL_OUT, self.COL_IN,
                    self.COL_MAX_OUT, self.COL_MIN_OUT,
                    self.COL_MAX_IN, self.COL_MIN_IN
                ])
            )
            st.dataframe(styled_abn, use_container_width=True)
            self.df_c4r_abn = df_c4r_probs[cols_show].copy()
        else:
            st.success("All C4R rows are within threshold.")
            self.df_c4r_abn = None


    def prepare(self):
        """เตรียม abnormal table สำหรับ Client board (ไม่ render UI)"""
        # 1) Normalize + validate
        self.df_client = self._normalize_cols(self.df_client_raw)
        self._validate_client_cols(self.df_client)

        # 2) Build mapping
        self.df_client = self._build_mapping_format(self.df_client)

        # 3) Load reference & merge
        self.df_ref = self._load_reference()
        self.df_merged = self._merge(self.df_client, self.df_ref)

        if self.df_merged.empty:
            self.df_abnormal = pd.DataFrame()
            self.df_abnormal_by_type = {}
            st.session_state["client_analyzer"] = self
            st.session_state["client_status"] = "No data"
            st.session_state["client_abn_count"] = 0
            return

        # 4) Select important cols & numeric cast
        self.df_result = self.df_merged[[
            "Site Name", "ME", "Measure Object",
            self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_OUT,
            self.COL_MAX_IN, self.COL_MIN_IN, self.COL_IN
        ]].copy()
        self.df_result = self._numeric_cast(
            self.df_result,
            [self.COL_OUT, self.COL_IN, self.COL_MAX_OUT, self.COL_MIN_OUT, self.COL_MAX_IN, self.COL_MIN_IN]
        )

        # 5) Detect abnormal rows (per link)
        mask_abn = (
            ((self.df_result[self.COL_OUT] > self.df_result[self.COL_MAX_OUT]) |
            (self.df_result[self.COL_OUT] < self.df_result[self.COL_MIN_OUT]) |
            (self.df_result[self.COL_IN] > self.df_result[self.COL_MAX_IN]) |
            (self.df_result[self.COL_IN] < self.df_result[self.COL_MIN_IN]))
            & (self.df_result[self.COL_IN] != -60)
            & (self.df_result[self.COL_OUT] != -60)
        )
        df_abn_all = self.df_result.loc[mask_abn].copy()

        # 6) แยก abnormal ต่อบอร์ด
        df_c2k_abn = df_abn_all[df_abn_all["Measure Object"].astype(str).str.startswith("C2K", na=False)].copy()
        df_c2l_abn = df_abn_all[df_abn_all["Measure Object"].astype(str).str.startswith("C2L", na=False)].copy()
        df_c4r_abn = df_abn_all[df_abn_all["Measure Object"].astype(str).str.startswith("C4R", na=False)].copy()



        # 7) Save abnormal results
        self.df_abnormal = df_abn_all
        self.df_abnormal_by_type = {
            "C2K": df_c2k_abn,
            "C2L": df_c2l_abn,
            "C4R": df_c4r_abn
        }

        # 8) Save status into session_state
        st.session_state["client_analyzer"] = self
        if self.df_abnormal.empty:
            st.session_state["client_status"] = "Normal"
        else:
            st.session_state["client_status"] = "Abnormal"
        st.session_state["client_abn_count"] = len(self.df_abnormal)
