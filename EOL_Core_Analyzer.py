import math
import streamlit as st
import pandas as pd
              # ✅ เพิ่มบรรทัดนี้
import plotly.express as px 



# region Base Analyzer for Loss
class LossAnalyzer:
    def __init__(
        self, 
        df_ref: pd.DataFrame | None = None, 
        df_raw_data: pd.DataFrame | None = None,
        ref_path: str | None = None,
    ):
        self.df_raw_data = df_raw_data
        # ใช้ df_ref ถ้ามี, ถ้าไม่มีลองโหลดจาก ref_path, ไม่งั้น None
        if df_ref is not None:
            self.df_ref = df_ref
        elif ref_path:
            self.df_ref = self._load_ref(ref_path)  # ← loader ในคลาส
        else:
            self.df_ref = None

    # ---------- loader (cache) ----------
    @staticmethod
    @st.cache_data(show_spinner=False)
    def _load_ref(path: str) -> pd.DataFrame:
        """
        อ่านไฟล์อ้างอิงจาก path → DataFrame
        cache ไว้เพื่อไม่ต้องอ่านซ้ำเมื่อมี rerun ของ Streamlit
        """
        try:
            df = pd.read_excel(path)
            df.columns = [str(c).strip() for c in df.columns]
            return df
        except Exception as e:
            st.error(f"Cannot load reference file from '{path}': {e}")
            raise

    # ------- Utilities -------
    @staticmethod
    def is_castable_to_float(x) -> bool:
        try:
            float(x)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def countDay(df_ref: pd.DataFrame):
        days = (len(df_ref.columns) - 11) / 4
        return int(days)

    @staticmethod
    def isDiffError(row):
        """
        ทำสีทั้งแถว:
        - error (แดง)    : Loss current - Loss EOL ≥ 2
        - flapping (เหลือง): Remark ไม่ว่าง
        """
        status = ""
        val = pd.to_numeric(row.get("Loss current - Loss EOL"), errors="coerce")

        if pd.notna(val) and val >= 2:
            status = "error"
        elif str(row.get("Remark", "") or "").strip() != "":
            status = "flapping"

        color_style = LossAnalyzer.getColor(status)
        return [color_style] * len(row)
    
    @staticmethod
    def getColor(status: str) -> str:
        color = ''
        if status == "error":
            color = 'background-color: #ff4d4d; color: white;'
        elif status == "flapping":
            color = 'background-color: #d6b346; color: white;'
        return color

    @staticmethod
    def draw_color_legend():
        st.markdown("""
            <div style='display: flex; justify-content: center; align-items: center; gap: 16px; margin-bottom: 1rem'>
                <div style='display: flex; justify-content: center; align-items: center; gap: 8px'>
                    <div style='background-color: #ff4d4d; width: 24px; height: 24px; border-radius: 8px;'></div>
                    <div style='text-align: center; color: #ff4d4d; font-size: 24px; font-weight: bold;'>
                        EOL not OK 
                    </div>
                </div>
                <div style='display: flex; justify-content: center; align-items: center; gap: 8px'>
                    <div style='background-color: #d6b346; width: 24px; height: 24px; border-radius: 8px;'></div>
                    <div style='text-align: center; color: #d6b346; font-size: 24px; font-weight: bold;'>
                        Fiber break occurs
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    @staticmethod
    def extract_eol_ref(df_ref: pd.DataFrame) -> pd.DataFrame:
        df = df_ref.copy()
        df.columns = [str(c).strip() for c in df.columns]

        required = ["Link Name", "EOL(dB)"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"Reference sheet must contain columns: {', '.join(required)} "
                f"(missing: {', '.join(missing)})"
            )

        out = df[required].copy()
        out["Link Name"] = out["Link Name"].astype(str).str.strip()
        out["EOL(dB)"]   = pd.to_numeric(out["EOL(dB)"], errors="coerce")
        out = out[out["Link Name"] != ""].reset_index(drop=True)
        return out


# region Analyzer for EOL
class EOLAnalyzer(LossAnalyzer):
    def extract_raw_data(self, df_raw_data: pd.DataFrame) -> pd.DataFrame:
        df_raw_data.columns = df_raw_data.columns.str.strip()
        df_atten = pd.DataFrame()
        source_port_col = df_raw_data["Source Port"]
        sink_port_col   = df_raw_data["Sink Port"]

        df_atten["Link Name"] = source_port_col + "_" + sink_port_col
        df_atten["Current Attenuation(dB)"] = df_raw_data["Optical Attenuation (dB)"]
        df_atten["Remark"] = df_atten["Current Attenuation(dB)"].apply(
            lambda x: "" if self.is_castable_to_float(x) else "Fiber Break"
        )
        return df_atten

    def calculate_eol_diff(self, df_eol: pd.DataFrame) -> pd.DataFrame:
        df_eol_diff = df_eol.copy()
        current_atten_col = pd.to_numeric(df_eol["Current Attenuation(dB)"], downcast="float", errors="coerce")
        eol_ref_col       = pd.to_numeric(df_eol["EOL(dB)"],                 downcast="float", errors="coerce")
        calculated_diff   = current_atten_col - eol_ref_col - 1  # ชดเชย +1 dB

        df_eol_diff["Loss current - Loss EOL"] = calculated_diff
        ordered_cols = ["Link Name", "EOL(dB)", "Current Attenuation(dB)", "Loss current - Loss EOL", "Remark"]
        return df_eol_diff[ordered_cols]
    
    def build_result_df(self):
        if self.df_ref is not None and self.df_raw_data is not None:
            df_eol_ref: pd.DataFrame = self.extract_eol_ref(self.df_ref)
            df_atten:   pd.DataFrame = self.extract_raw_data(self.df_raw_data)

            joined_df = df_eol_ref.join(df_atten.set_index("Link Name"), on="Link Name")
            df_result = self.calculate_eol_diff(joined_df)
            return df_result
        return pd.DataFrame()
    
    def get_me_names(self, df_result: pd.DataFrame) -> list[str]:
        link_names = df_result["Link Name"].astype(str).tolist()
        me_names = [name.split("-")[0] if "-" in name else name for name in link_names]
        return list(sorted(set(me_names)))  # ✅ unique + sorted
    
    def get_filtered_result(self, df_result: pd.DataFrame, selected_me_name: str) -> pd.DataFrame:
        if not selected_me_name:
            return df_result.reset_index(drop=True)
        mask = df_result["Link Name"].astype(str).str.contains(selected_me_name, na=False)
        return df_result[mask].reset_index(drop=True)
    
    def get_selected_me_name(self, df_result):
        me_names = self.get_me_names(df_result)
        selected_me_name = st.selectbox(
            "Managed Element (EOL)",
            me_names,
            index=None,
            placeholder="Choose options",
            key="eol_me_select"
        )
        return selected_me_name

    def process(self, show_table: bool = True, enable_filter: bool = True):   # ✅ เพิ่ม enable_filter
        if self.df_ref is not None and self.df_raw_data is not None:
            df_result = self.build_result_df()

            if enable_filter:
                selected_me_name = self.get_selected_me_name(df_result)
                df_filtered = self.get_filtered_result(df_result, selected_me_name)
            else:
                df_filtered = df_result

            if "Remark" in df_filtered.columns:
                df_filtered["Remark"] = df_filtered["Remark"].fillna("")

            # ---------- ตารางหลัก ----------
            if show_table:
                st.dataframe(df_filtered.style.apply(self.isDiffError, axis=1), hide_index=True)
                self.draw_color_legend()

            # ... (ส่วน KPI, Donut, Problem list เหมือนเดิม)

            # ---------- KPI ----------
            status_list = []
            for _, row in df_filtered.iterrows():
                val = pd.to_numeric(row.get("Loss current - Loss EOL"), errors="coerce")
                remark = str(row.get("Remark", "")).strip()

                if remark != "":
                    status_list.append("EOL Fiber Break")
                elif pd.notna(val) and val >= 2:
                    status_list.append("EOL Excess Loss")
                else:
                    status_list.append("EOL Normal")

            df_status = pd.DataFrame({"Status": status_list})
            summary_counts = df_status["Status"].value_counts()

            normal_cnt = int(summary_counts.get("EOL Normal", 0))
            excess_cnt = int(summary_counts.get("EOL Excess Loss", 0))
            break_cnt  = int(summary_counts.get("EOL Fiber Break", 0))
            total_cnt  = normal_cnt + excess_cnt + break_cnt  

            st.markdown("### EOL Link Status Overview")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("EOL Normal", f"{normal_cnt}")
            col2.metric("EOL Excess Loss", f"{excess_cnt}")
            col3.metric("EOL Fiber Break", f"{break_cnt}")
            col4.metric("EOL Total", f"{total_cnt}")

            # ---------- Donut ----------
            fig = px.pie(
                df_status,
                names="Status",
                hole=0.5,
                color="Status",
                color_discrete_map={
                    "EOL Normal": "green",
                    "EOL Excess Loss": "red",
                    "EOL Fiber Break": "gold"
                }
            )
            fig.update_traces(textinfo="value+label")
            fig.add_annotation(dict(
                text=f"Total<br>{total_cnt}",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=18, color="black"),
                xanchor="center", yanchor="middle"
            ))
            st.plotly_chart(fig, use_container_width=True)

            # ---------- Problem Links ----------
            st.subheader("EOL Excess Loss")
            df_excess = df_filtered.loc[[s == "EOL Excess Loss" for s in status_list]].reset_index(drop=True)
            if df_excess.empty:
                st.success("No EOL Excess Loss links found.")
            else:
                def hl_loss(val):
                    return "background-color: #ffe6e6"  # แดงอ่อน

                st.dataframe(
                    df_excess[["Link Name", "EOL(dB)", "Current Attenuation(dB)", "Loss current - Loss EOL"]]
                        .style.applymap(hl_loss, subset=["Loss current - Loss EOL"]),
                    hide_index=True,
                    use_container_width=True
                )

            # ---------------- EOL Fiber Break ----------------
            st.subheader("EOL Fiber Break")
            df_break = df_filtered.loc[[s == "EOL Fiber Break" for s in status_list]].reset_index(drop=True)
            if df_break.empty:
                st.success("No EOL Fiber Break links found.")
            else:
                def hl_remark(val):
                    if str(val).strip() != "":
                        return "background-color: #fff8cc"  # เหลืองอ่อน
                    return ""

                st.dataframe(
                    df_break[["Link Name", "Remark"]]
                        .style.applymap(hl_remark, subset=["Remark"]),
                    hide_index=True,
                    use_container_width=True
                )
            self.abnormal_tables = {
                "EOL Excess Loss": df_excess,
                "EOL Fiber Break": df_break,
            }               

    @property
    def df_abnormal(self):
        if hasattr(self, "abnormal_tables"):
            try:
                return pd.concat(self.abnormal_tables.values(), ignore_index=True)
            except ValueError:
                return pd.DataFrame()
        return pd.DataFrame()

    @property
    def df_abnormal_by_type(self):
        return getattr(self, "abnormal_tables", {})


    def prepare(self):
        """
        เตรียม abnormal_tables (Excess + Fiber Break) 
        โดยไม่ render UI
        """
        if self.df_ref is not None and self.df_raw_data is not None:
            df_result = self.build_result_df()
            print("[DEBUG][EOL] build_result_df shape:", df_result.shape)

            status_list = []
            for _, row in df_result.iterrows():
                val = pd.to_numeric(row.get("Loss current - Loss EOL"), errors="coerce")
                remark = str(row.get("Remark", "")).strip()
                if remark != "":
                    status_list.append("EOL Fiber Break")
                elif pd.notna(val) and val >= 2:
                    status_list.append("EOL Excess Loss")
                else:
                    status_list.append("EOL Normal")

            df_excess = df_result.loc[[s == "EOL Excess Loss" for s in status_list]].reset_index(drop=True)
            df_break  = df_result.loc[[s == "EOL Fiber Break" for s in status_list]].reset_index(drop=True)

            self.abnormal_tables = {
                "EOL Excess Loss": df_excess,
                "EOL Fiber Break": df_break,
            }

            print(f"[DEBUG][EOL] Excess={len(df_excess)}, Break={len(df_break)}")


class CoreAnalyzer(EOLAnalyzer):
    def calculate_loss_between_core(self, df_result: pd.DataFrame) -> pd.DataFrame:
        forward_direction = df_result["Loss current - Loss EOL"].iloc[::2].values
        reverse_direction = df_result["Loss current - Loss EOL"].iloc[1::2].values
        loss_between_core = [abs(f - r) for f, r in zip(forward_direction, reverse_direction)]
        loss_between_core = ["--" if pd.isna(value) else round(value, 2) for value in loss_between_core]

        df_loss_between_core = pd.DataFrame()
        df_loss_between_core["Link Name"] = df_result["Link Name"]
        df_loss_between_core["Loss between core"] = [x for x in loss_between_core for _ in range(2)]
        return df_loss_between_core
    
    @staticmethod
    def getColorCondition(value, threshold=2) -> str:
        if value == "--":
            return "flapping"
        elif value > threshold:
            return "error"
        return ""

    def build_loss_table_body(self, link_names, loss_values) -> str:
        table_body = ""
        for i in range(len(link_names)):
            status = CoreAnalyzer.getColorCondition(loss_values[i])
            color  = LossAnalyzer.getColor(status)
            merged_cells = ""
            if i % 2 == 0:
                formated_value = loss_values[i]
                if formated_value != "--":
                    formated_value = "{:.2f}".format(loss_values[i])
                merged_cells = f"""
                    <td style='border: 1px solid #ccc; padding: 4px 8px; text-align: center; {color}' rowspan=2>
                        {formated_value}
                    </td>
                """.strip()
            table_body += f"""
                <tr>
                    <td style='border: 1px solid #ccc; padding: 4px 8px; {color}'>
                        {link_names[i]}
                    </td>{merged_cells}
                </tr>
            """.strip()
        return table_body
    
    def build_loss_table(self, link_names, loss_values) -> str:
        table_body = self.build_loss_table_body(link_names, loss_values)
        html = f"""
            <div style="max-height: 500px; overflow-y: auto; border: 1px solid #ccc; border-radius: 0.5rem;">
                <table style="border-collapse: collapse; width: 100%; text-align: left; font-family: 'Source Sans', sans-serif; font-size: 14px;">
                    <thead style="background-color: #f2f2f2; color: #000000;">
                        <tr>
                            <th style="border: 1px solid #ccc; padding: 4px 8px;">Link Name</th>
                            <th style="border: 1px solid #ccc; padding: 4px 8px;">Loss between core</th>
                        </tr>
                    </thead>
                    <tbody style="background-color: #ffffff; color: #000000;">
                        {table_body}
                    </tbody>
                </table>
            </div>
        """
        return html

    def get_selected_me_name(self, df_result):
        me_names = self.get_me_names(df_result)
        selected_me_name = st.selectbox(
            "Managed Element (Core)",
            me_names,
            index=None,
            placeholder="Choose options",
            key="core_me_select"
        )
        return selected_me_name

    def process(self, show_table: bool = True, enable_filter: bool = True):   # ✅ เพิ่ม enable_filter
        if self.df_ref is not None and self.df_raw_data is not None:
            df_result = self.build_result_df()

            # ✅ เลือกว่าจะ filter หรือไม่
            if enable_filter:
                selected_me_name = self.get_selected_me_name(df_result)
                df_filtered = self.get_filtered_result(df_result, selected_me_name)
            else:
                df_filtered = df_result

            df_loss_between_core = self.calculate_loss_between_core(df_filtered)
            link_names  = df_loss_between_core["Link Name"].tolist()
            loss_values = df_loss_between_core["Loss between core"].tolist()

            # ---------- ตารางหลัก ----------
            if show_table:
                html = self.build_loss_table(link_names, loss_values)
                st.markdown(html, unsafe_allow_html=True)

                # Legend
                st.markdown("""
                    <div style='display: flex; justify-content: center; align-items: center; gap: 32px; margin: 1rem 0;'>
                        <div style='display: flex; align-items: center; gap: 8px'>
                            <div style='background-color: #ff4d4d; width: 24px; height: 24px; border-radius: 4px;'></div>
                            <div style='color: #ff4d4d; font-size: 24px; font-weight: bold;'>Loss not OK </div>
                        </div>
                        <div style='display: flex; align-items: center; gap: 8px'>
                            <div style='background-color: #d6b346; width: 24px; height: 24px; border-radius: 4px;'></div>
                            <div style='color: #d6b346; font-size: 24px; font-weight: bold;'>Fiber break occurs</div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

            # ---------- KPI ----------
            status_list = []
            for v in loss_values:
                if v == "--":
                    status_list.append("Core Fiber Break")
                elif pd.notna(v) and v > 2:
                    status_list.append("Core Loss Excess")
                else:
                    status_list.append("Core Normal")

            df_summary = pd.DataFrame({"Status": status_list})
            summary_counts = df_summary["Status"].value_counts()

            ok_cnt    = int(summary_counts.get("Core Normal", 0))
            notok_cnt = int(summary_counts.get("Core Loss Excess", 0))
            fiber_cnt = int(summary_counts.get("Core Fiber Break", 0))
            total_cnt = ok_cnt + notok_cnt + fiber_cnt  

            st.markdown("### Core Link Status Overview")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Core Normal", f"{ok_cnt}")
            col2.metric("Core Loss Excess", f"{notok_cnt}")
            col3.metric("Core Fiber Break", f"{fiber_cnt}")
            col4.metric("Core Total", f"{total_cnt}")

            # ---------- Donut ----------
            fig = px.pie(
                df_summary, 
                names="Status", 
                hole=0.5, 
                color="Status",
                color_discrete_map={
                    "Core Normal": "green",
                    "Core Loss Excess": "red",
                    "Core Fiber Break": "gold"
                }
            )
            fig.update_traces(textinfo="value+label")
            fig.add_annotation(dict(
                text=f"Total<br>{total_cnt}",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=18, color="black"),
                xanchor="center", yanchor="middle"
            ))
            st.plotly_chart(fig, use_container_width=True)

            
            # ---------- Problem Links ----------
            st.markdown("### Problem Links")
            df_problem = pd.DataFrame({
                "Link Name": link_names,
                "Loss between core": loss_values,
                "Status": status_list
            })

            # ✅ สไตล์: สลับสีทั้งแถวเป็นคู่ (A→B, B→A)
            def style_pair(row):
                pair_index = row.name // 2  # 0,0 | 1,1 | 2,2 ...
                base = "#ffffff" if (pair_index % 2 == 0) else "#f2f2f2"
                return [f"background-color: {base}"] * len(row)

            # ✅ ไฮไลต์คอลัมน์ Loss between core
            def hl_red(val):
                return "background-color: #ffe6e6"  # แดงอ่อน (Excess)

            def hl_yellow(val):
                return "background-color: #fff8cc"  # เหลืองอ่อน (Fiber Break)

           
            # ---------------- Core Loss Excess ----------------
            st.subheader("Core Loss Excess")
            df_loss = pd.DataFrame({
                "Link Name": [ln for ln, stt in zip(link_names, status_list) if stt == "Core Loss Excess"],
                "Loss between core": [lv for lv, stt in zip(loss_values, status_list) if stt == "Core Loss Excess"]
            }).reset_index(drop=True)

            if df_loss.empty:
                st.success("No Core Loss Excess links found.")
            else:
                st.dataframe(
                    df_loss
                        .style.apply(style_pair, axis=1)          # สลับสีทั้งแถวเป็นคู่
                        .applymap(hl_red, subset=["Loss between core"]), 
                    hide_index=True,
                    use_container_width=True
                )

            # ---------------- Core Fiber Break ----------------
            st.subheader("Core Fiber Break")
            df_break = pd.DataFrame({
                "Link Name": [ln for ln, stt in zip(link_names, status_list) if stt == "Core Fiber Break"],
                "Loss between core": [
                    "Fiber Break" if lv == "--" else lv
                    for lv, stt in zip(loss_values, status_list) if stt == "Core Fiber Break"
                ]
            }).reset_index(drop=True)

            if df_break.empty:
                st.success("No Core Fiber Break links found.")
            else:
                st.dataframe(
                    df_break
                        .style.apply(style_pair, axis=1)
                        .applymap(hl_yellow, subset=["Loss between core"]),
                    hide_index=True,
                    use_container_width=True
                )

            self.abnormal_tables = {
                "Core Loss Excess": df_loss,
                "Core Fiber Break": df_break,
            }

    @property
    def df_abnormal(self):
        if hasattr(self, "abnormal_tables"):
            try:
                return pd.concat(self.abnormal_tables.values(), ignore_index=True)
            except ValueError:
                return pd.DataFrame()
        return pd.DataFrame()

    @property
    def df_abnormal_by_type(self):
        return getattr(self, "abnormal_tables", {})

    def prepare(self):
        """
        เตรียม abnormal_tables (Loss Excess + Fiber Break) 
        โดยไม่ render UI
        """
        if self.df_ref is not None and self.df_raw_data is not None:
            df_result = self.build_result_df()
            print("[DEBUG][Core] build_result_df shape:", df_result.shape)

            df_loss_between_core = self.calculate_loss_between_core(df_result)
            link_names  = df_loss_between_core["Link Name"].tolist()
            loss_values = df_loss_between_core["Loss between core"].tolist()

            status_list = []
            for v in loss_values:
                if v == "--":
                    status_list.append("Core Fiber Break")
                elif pd.notna(v) and v > 2:
                    status_list.append("Core Loss Excess")
                else:
                    status_list.append("Core Normal")

            df_loss = pd.DataFrame({
                "Link Name": [ln for ln, stt in zip(link_names, status_list) if stt == "Core Loss Excess"],
                "Loss between core": [lv for lv, stt in zip(loss_values, status_list) if stt == "Core Loss Excess"]
            }).reset_index(drop=True)

            df_break = pd.DataFrame({
                "Link Name": [ln for ln, stt in zip(link_names, status_list) if stt == "Core Fiber Break"],
                "Loss between core": [
                    "Fiber Break" if lv == "--" else lv
                    for lv, stt in zip(loss_values, status_list) if stt == "Core Fiber Break"
                ]
            }).reset_index(drop=True)

            self.abnormal_tables = {
                "Core Loss Excess": df_loss,
                "Core Fiber Break": df_break,
            }

            print(f"[DEBUG][Core] LossExcess={len(df_loss)}, Break={len(df_break)}")
