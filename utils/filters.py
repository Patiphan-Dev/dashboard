# utils/filters.py
import streamlit as st
import pandas as pd
from typing import Dict, List, Tuple

def cascading_filter(
    df: pd.DataFrame,
    cols: List[str],
    *,
    ns: str = "flt",
    labels: Dict[str, str] | None = None,
    clear_text: str = "Clear Filters",
) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """
    ฟิลเตอร์แบบไล่ชั้น (cascading): สร้าง multiselect ต่อเนื่องทีละคอลัมน์
    คืนค่า: (DataFrame ที่ถูกกรองแล้ว, selections dict)
    """

    if labels is None:
        labels = {}

    # เตรียม state
    for c in cols:
        st.session_state.setdefault(f"{ns}_f_{c}", [])

    # คอลัมน์ที่มีอยู่จริงเท่านั้น
    active_cols = [c for c in cols if c in df.columns]
    if not active_cols:
        return df.reset_index(drop=True), {}

    # สร้าง options ทีละชั้นด้วย mask สะสม
    masks = [pd.Series(True, index=df.index)]
    options_per_col = []
    for i, c in enumerate(active_cols):
        m = masks[-1]
        opts = sorted(df.loc[m, c].dropna().astype(str).unique())
        options_per_col.append(opts)

        # prune ค่าเลือกที่ไม่อยู่ใน opts (กัน selection ค้าง)
        valid_sel = [x for x in st.session_state[f"{ns}_f_{c}"] if x in opts]
        st.session_state[f"{ns}_f_{c}"] = valid_sel

        # อัปเดต mask สำหรับคอลัมน์ถัดไป
        if valid_sel:
            masks.append(m & df[c].astype(str).isin(valid_sel))
        else:
            masks.append(m)

    # วาด widgets เป็นแถวเดียว + ปุ่ม Clear
    cols_widgets = st.columns([1] * len(active_cols) + [0.8])
    for i, c in enumerate(active_cols):
        with cols_widgets[i]:
            st.multiselect(
                labels.get(c, c),
                options_per_col[i],
                key=f"{ns}_f_{c}",
            )

    def _clear():
        for c in active_cols:
            st.session_state.pop(f"{ns}_f_{c}", None)

    with cols_widgets[-1]:
        st.button(clear_text, on_click=_clear)

    # สร้าง final mask จาก selections ทั้งหมด
    final_mask = pd.Series(True, index=df.index)
    selections: Dict[str, List[str]] = {}
    for c in active_cols:
        sel = st.session_state.get(f"{ns}_f_{c}", [])
        selections[c] = sel
        if sel:
            final_mask &= df[c].astype(str).isin(sel)

    return df[final_mask].reset_index(drop=True), selections
