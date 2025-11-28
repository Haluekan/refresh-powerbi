import pyodbc
import pandas as pd
import logging
import os
import time  # <--- ✅ 1. เพิ่ม time สำหรับระบบ Retry

# ----------------------------------------------------------------------
# ⚙️ ตั้งค่าระบบ Retry (ลองใหม่)
# ----------------------------------------------------------------------
MAX_RETRIES = 10       # ลองใหม่สูงสุด 10 ครั้ง
RETRY_DELAY = 60       # รอ 60 วินาที ก่อนเริ่มรอบใหม่

# ----------------------------------------------------------------------
# 2. สร้างที่อยู่เต็ม (Absolute Paths)
# ----------------------------------------------------------------------

# ที่อยู่ของไฟล์ Log
log_file_path = r"D:\OneDrive - Datamars SA\General - IT (Datamars Thailand)\8. Power Platform\Power BI\DATA\export data\logfile\srwilog.log"

# ที่อยู่ของไฟล์ Excel
output_file_path = r"D:\OneDrive - Datamars SA\General - IT (Datamars Thailand)\8. Power Platform\Power BI\DATA\export data\SRWI work order - 2025.xlsx"

# ----------------------------------------------------------------------
# 3. ตั้งค่าการบันทึก Log
# ----------------------------------------------------------------------
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# ----------------------------------------------------------------------
# 1. ตั้งค่าการเชื่อมต่อ (✅ ใช้ Driver 17 + เพิ่ม Timeout)
# ----------------------------------------------------------------------
connection_string = (
    # ✅ เปลี่ยนเป็น Driver 17 เพื่อแก้ Protocol Error
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=sap-temp.datamars.local;"
    "DATABASE=DB_SAP_DMTHAI;"
    "UID=ro;"
    "PWD=DMquery!;"
    "Timeout=1800;" # ✅ เพิ่ม Timeout เป็น 30 นาที กันสายหลุด
)

# ----------------------------------------------------------------------
# ข้อมูล Views และ Sheet ที่ต้องการ
# ----------------------------------------------------------------------
data_to_export = [
    {
        "view_name": "vw_SRWI_2025",
        "sheet_name": "Data"
    },
    {
        "view_name": "vw_MatPmt_2025",
        "sheet_name": "vw_MatPmt_2025 (2)"
    }
]

# ----------------------------------------------------------------------
# 4. ส่วนเชื่อมต่อและดึงข้อมูล (พร้อมระบบ Retry Loop)
# ----------------------------------------------------------------------
logging.info(f"--- [!] เริ่มการทำงาน (โหมด: SRWI Excel + Retry + Chunking) ---")
logging.info(f"จะบันทึก Log ไปที่: {log_file_path}")
logging.info(f"จะบันทึก Excel ไปที่: {output_file_path}")

# ✅ เริ่มวนลูป Retry
for attempt in range(1, MAX_RETRIES + 1):
    try:
        logging.info(f"🔄 ความพยายามครั้งที่ {attempt}/{MAX_RETRIES}...")
        logging.info(f"กำลังเชื่อมต่อกับฐานข้อมูล...")
        
        with pyodbc.connect(connection_string) as connection:
            logging.info("เชื่อมต่อสำเร็จ! (ใช้ Driver 17)")
            
            # เปิด ExcelWriter เพื่อเตรียมเขียนหลายชีต
            # (ต้องเปิดใหม่ทุกรอบ Retry เพื่อความปลอดภัยของไฟล์)
            with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                
                any_data_written = False
                
                for item in data_to_export:
                    view_name = item["view_name"]
                    sheet_name = item["sheet_name"]
                    sql_query = f"SELECT * FROM {view_name};"
                    
                    logging.info(f"กำลังดึงข้อมูลจาก View: {view_name} (ชีต: {sheet_name}) แบบ Chunking...")
                    
                    # ✅ Chunking Logic
                    chunk_size = 50000
                    data_iterator = pd.read_sql_query(sql_query, connection, chunksize=chunk_size)
                    
                    chunks_list = []
                    total_rows = 0
                    
                    for i, chunk in enumerate(data_iterator):
                        chunks_list.append(chunk)
                        total_rows += len(chunk)
                        logging.info(f"   ...โหลดก้อนที่ {i+1} เรียบร้อย ({len(chunk)} แถว) | รวม {total_rows} แถว")
                    
                    if total_rows > 0:
                        logging.info(f"โหลดครบ! กำลังเขียนลงชีต '{sheet_name}'...")
                        
                        full_df = pd.concat(chunks_list)
                        
                        full_df.to_excel(
                            writer, 
                            sheet_name=sheet_name,
                            index=False
                        )
                        any_data_written = True
                        logging.info(f"เขียนลงชีต '{sheet_name}' สำเร็จ ({total_rows} แถว)")
                    else:
                        logging.warning(f"ไม่พบข้อมูลใน '{view_name}' (0 แถว) - ข้ามชีตนี้")

                if any_data_written:
                    logging.info(f"✅ สำเร็จ! บันทึกไฟล์ Excel เรียบร้อย")
                else:
                    logging.warning(f"⚠️ ไม่มีข้อมูลในทุก View ไฟล์ Excel อาจว่างเปล่า")

        # ✅ ถ้าทำงานมาถึงตรงนี้โดยไม่มี Error แปลว่าสำเร็จ -> จบการทำงาน
        logging.info("--- [!] จบการทำงาน (สำเร็จ) --- \n")
        break 

    except Exception as e:
        # ❌ ถ้าเกิด Error ในรอบนี้
        logging.error(f"❌ ล้มเหลวในรอบที่ {attempt}: {e}")
        
        if 'HY000' in str(e):
             logging.error("!!! Protocol Error: เช็ก Driver 17 ด่วน !!!")
        
        if attempt < MAX_RETRIES:
            logging.info(f"⏳ จะลองใหม่ในอีก {RETRY_DELAY} วินาที...")
            time.sleep(RETRY_DELAY)  # ✅ รอเวลาก่อนเริ่มรอบใหม่
        else:
            logging.critical(f"⛔ ล้มเหลวครบ {MAX_RETRIES} ครั้งแล้ว! ขอยุติการทำงาน")
            logging.info("--- [!] จบการทำงาน (ล้มเหลว) --- \n")