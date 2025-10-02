# test_supabase.py
from supabase import create_client

# ✅ เปลี่ยนเป็น URL + Key ของคุณจริง ๆ
url = "https://qkipiuxqeiwgtmhmvfyz.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFraXBpdXhxZWl3Z3RtaG12Znl6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTkzMTM0NjYsImV4cCI6MjA3NDg4OTQ2Nn0.y-l0P-ghAIGL78heZfq9woy7DdhYPn71G9HmoMnTRAw"

supabase = create_client(url, key)

# ทดสอบดึงข้อมูลจาก table uploads
try:
    res = supabase.table("uploads").select("*").execute()
    print("✅ Supabase connected successfully")
    print("Data in uploads:", res.data)
except Exception as e:
    print("❌ Supabase connection failed:", e)
