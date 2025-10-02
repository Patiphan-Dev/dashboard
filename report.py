from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
)
from reportlab.lib import colors
from reportlab.lib.colors import HexColor

import io
from datetime import datetime
import pandas as pd


def generate_report(all_abnormal: dict):
    """
    สร้าง PDF Report รวม FAN + CPU + MSU
    """

    # ===== Buffer & Document =====
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4))

    styles = getSampleStyleSheet()

    # ===== Custom Styles =====
    title_center = ParagraphStyle(
        "TitleCenter", parent=styles["Heading1"], alignment=1, spaceAfter=20
    )
    date_center = ParagraphStyle(
        "DateCenter", parent=styles["Normal"], alignment=1, spaceAfter=12
    )
    section_title_left = ParagraphStyle(
        "SectionTitleLeft", parent=styles["Heading2"], alignment=0, spaceAfter=6
    )
    normal_left = ParagraphStyle(
        "NormalLeft", parent=styles["Normal"], alignment=0, spaceAfter=12
    )

    elements = []

    # ===== Title & Date =====
    elements.append(Paragraph("3BB Network Inspection Report", title_center))
    elements.append(
        Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", date_center)
    )
    elements.append(Spacer(1, 24))

    # ===== Sections (CPU มาก่อน FAN) =====
    section_order = ["CPU", "FAN", "MSU", "Client"]
    light_red = HexColor("#FF9999")
    text_black = colors.black

    for section_name in section_order:
        abn_dict = all_abnormal.get(section_name, {})

        elements.append(Paragraph(f"{section_name} Performance", section_title_left))

        if not abn_dict:
            elements.append(Paragraph("✅ No abnormal values found.", normal_left))
            elements.append(Spacer(1, 12))
            continue

        for subtype, df in abn_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            # Section Title
            elements.append(Paragraph(f"{subtype} – Abnormal Rows", section_title_left))
            elements.append(Spacer(1, 6))

            df_show = df.copy()

            # ===== Filter columns =====
            if section_name == "FAN":
                cols_to_show = [
                    "Site Name", "ME", "Measure Object",
                    "Maximum threshold", "Minimum threshold",
                    "Value of Fan Rotate Speed(Rps)"
                ]
                df_show = df_show[[c for c in cols_to_show if c in df_show.columns]]

            elif section_name == "CPU":
                cols_to_show = [
                    "Site Name", "ME", "Measure Object",
                    "Maximum threshold", "Minimum threshold",
                    "CPU utilization ratio"
                ]
                df_show = df_show[[c for c in cols_to_show if c in df_show.columns]]

            elif section_name == "MSU":
                cols_to_show = [
                    "Site Name", "ME", "Measure Object",
                    "Maximum threshold", "Laser Bias Current(mA)"
                ]
                df_show = df_show[[c for c in cols_to_show if c in df_show.columns]]

            elif section_name == "Client":
                cols_to_show = [
                    "Site Name", "ME", "Measure Object",
                    "Maximum threshold(out)", "Minimum threshold(out)", "Output Optical Power (dBm)",
                    "Maximum threshold(in)", "Minimum threshold(in)", "Input Optical Power(dBm)"
                ]
                df_show = df_show[[c for c in cols_to_show if c in df_show.columns]]


            # ===== Build table_data =====
            if df_show.empty:
                elements.append(Paragraph("⚠️ Data exists but no valid columns to display.", normal_left))
                elements.append(Spacer(1, 12))
                continue

            table_data = [list(df_show.columns)] + df_show.astype(str).values.tolist()
            table = Table(table_data, repeatRows=1)

            style_cmds = [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
            ]

            # ===== Highlight logic =====
            if section_name == "CPU" and "CPU utilization ratio" in cols_to_show:
                col_idx = cols_to_show.index("CPU utilization ratio")
                if col_idx < len(df_show.columns):
                    style_cmds.append(("BACKGROUND", (col_idx, 1), (col_idx, -1), light_red))
                    style_cmds.append(("TEXTCOLOR", (col_idx, 1), (col_idx, -1), text_black))

            elif section_name == "FAN" and "Value of Fan Rotate Speed(Rps)" in cols_to_show:
                col_idx = cols_to_show.index("Value of Fan Rotate Speed(Rps)")
                if col_idx < len(df_show.columns):
                    style_cmds.append(("BACKGROUND", (col_idx, 1), (col_idx, -1), light_red))
                    style_cmds.append(("TEXTCOLOR", (col_idx, 1), (col_idx, -1), text_black))

            elif section_name == "MSU" and "Laser Bias Current(mA)" in cols_to_show:
                col_idx = cols_to_show.index("Laser Bias Current(mA)")
                if col_idx < len(df_show.columns):
                    style_cmds.append(("BACKGROUND", (col_idx, 1), (col_idx, -1), light_red))
                    style_cmds.append(("TEXTCOLOR", (col_idx, 1), (col_idx, -1), text_black))

          
          
            elif section_name == "Client":
                nrows = len(df_show) + 1   # header + data
                ncols = len(df_show.columns)
                col_map = {c: i for i, c in enumerate(df_show.columns)}  # ✅ สร้าง map คอลัมน์จริง

                for ridx, row in df_show.iterrows():
                    # Output check
                    try:
                        v = float(row.get("Output Optical Power (dBm)", float("nan")))
                        lo = float(row.get("Minimum threshold(out)", float("nan")))
                        hi = float(row.get("Maximum threshold(out)", float("nan")))
                        if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                            cidx = col_map.get("Output Optical Power (dBm)")
                            if cidx is not None and 0 <= cidx < ncols and 0 <= ridx+1 < nrows:
                                style_cmds.append(("BACKGROUND", (cidx, ridx+1), (cidx, ridx+1), light_red))
                                style_cmds.append(("TEXTCOLOR", (cidx, ridx+1), (cidx, ridx+1), text_black))
                    except:
                        pass

                    # Input check
                    try:
                        v = float(row.get("Input Optical Power(dBm)", float("nan")))
                        lo = float(row.get("Minimum threshold(in)", float("nan")))
                        hi = float(row.get("Maximum threshold(in)", float("nan")))
                        if pd.notna(v) and pd.notna(lo) and pd.notna(hi) and (v < lo or v > hi):
                            cidx = col_map.get("Input Optical Power(dBm)")
                            if cidx is not None and 0 <= cidx < ncols and 0 <= ridx+1 < nrows:
                                style_cmds.append(("BACKGROUND", (cidx, ridx+1), (cidx, ridx+1), light_red))
                                style_cmds.append(("TEXTCOLOR", (cidx, ridx+1), (cidx, ridx+1), text_black))
                    except:
                        pass


            # ===== Apply style & append =====
            table.setStyle(TableStyle(style_cmds))
            elements.append(table)
            elements.append(Spacer(1, 18))

    # ===== Build Document =====
    doc.build(elements)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf
