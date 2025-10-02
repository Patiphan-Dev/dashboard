import sqlite3
import pandas as pd

DB_FILE = "files.db"
CSV_FILE = "uploads_data.csv"

try:
    conn = sqlite3.connect(DB_FILE)
    # ดึงข้อมูลจากตาราง uploads ทั้งหมด
    df = pd.read_sql_query("SELECT upload_date, orig_filename, stored_path, created_at FROM uploads", conn)
    conn.close()

    # บันทึกเป็นไฟล์ CSV
    df.to_csv(CSV_FILE, index=False)
    print(f"✅ Exported {len(df)} rows from {DB_FILE} to {CSV_FILE}")

except Exception as e:
    print(f"❌ Error during export: {e}")