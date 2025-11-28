import pyodbc
import pandas as pd
import logging
import os

# ----------------------------------------------------------------------
# 2. สร้างที่อยู่เต็ม (Absolute Paths)
# ----------------------------------------------------------------------

# ที่อยู่ของไฟล์ Log
log_file_path = r"D:\OneDrive - Datamars SA\General - IT (Datamars Thailand)\8. Power Platform\Power BI\DATA\export data\logfile\revenueslog.log"

# ที่อยู่ของไฟล์ Excel (ผมเอาออกจากโฟลเดอร์ logfile ให้นะครับ น่าจะตั้งใจวางไว้ข้างนอก)
output_file_path = r"D:\OneDrive - Datamars SA\General - IT (Datamars Thailand)\8. Power Platform\Power BI\DATA\export data\SalesRevenues.xlsx"

# ชื่อชีต
your_sheet_name = "vw_SalesRevenues"

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
# 1. ตั้งค่าการเชื่อมต่อ (Driver 17 + Timeout)
# ----------------------------------------------------------------------
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"  
    "SERVER=sap-temp.datamars.local;"
    "DATABASE=DB_SAP_DMTHAI;"
    "UID=ro;"
    "PWD=DMquery!;"
    "Timeout=1800;" # 30 นาที
)

view_name = "vw_SalesRevenues"
sql_query = f"SELECT * FROM {view_name};"

# ----------------------------------------------------------------------
# 4. ส่วนเชื่อมต่อและดึงข้อมูล (โหมด Excel แท้ + Chunking)
# ----------------------------------------------------------------------
logging.info(f"--- [!] เริ่มการทำงาน (โหมด: Excel + Chunking + Driver 17) ---")
logging.info(f"จะบันทึก Log ไปที่: {log_file_path}")
logging.info(f"จะบันทึก Excel ไปที่: {output_file_path}")

try:
    logging.info(f"กำลังเชื่อมต่อกับฐานข้อมูล...")
    with pyodbc.connect(connection_string) as connection:
        logging.info("เชื่อมต่อสำเร็จ! (ใช้ Driver 17)")
        logging.info(f"กำลังดึงข้อมูลจาก View: {view_name} (แบบ Chunking)...")
        
        # ✅ Chunking: ทยอยดึงทีละ 50,000 แถว เพื่อแก้เน็ตหลุด/Ram เต็ม
        chunk_size = 50000
        data_iterator = pd.read_sql_query(sql_query, connection, chunksize=chunk_size)
        
        # สร้าง List ไว้พักข้อมูลแต่ละก้อน
        chunks_list = []
        total_rows = 0
        
        for i, chunk in enumerate(data_iterator):
            chunks_list.append(chunk)
            total_rows += len(chunk)
            logging.info(f"   ...โหลดก้อนที่ {i+1} เรียบร้อย ({len(chunk)} แถว) | รวม {total_rows} แถว")
            
        if total_rows > 0:
            logging.info(f"โหลดครบแล้ว! กำลังรวมข้อมูลและเขียนลงไฟล์ Excel...")
            
            # รวมทุกก้อนเป็น DataFrame เดียว (ใน Ram)
            full_df = pd.concat(chunks_list)
            
            # ✅ เขียนลง Excel จริงๆ (ใช้ to_excel ไม่ใช่ to_csv)
            full_df.to_excel(
                output_file_path, 
                index=False, 
                sheet_name=your_sheet_name,
                engine='xlsxwriter'
            )
            
            logging.info(f"✅ สำเร็จ! บันทึกข้อมูล {total_rows} แถว ลงในไฟล์ '{output_file_path}' เรียบร้อย")
        else:
            logging.warning(f"⚠️ ไม่พบข้อมูลใน View '{view_name}' (0 แถว)")

except pyodbc.Error as ex:
    logging.error(f"เกิดข้อผิดพลาด SQL/Driver: {ex}")
    if 'HY000' in str(ex):
        logging.error("!!! Protocol Error: กรุณาตรวจสอบว่าติดตั้ง 'ODBC Driver 17 for SQL Server' บนเครื่องแล้วหรือยัง")
except PermissionError:
    error_message = f"เกิดข้อผิดพลาด (PermissionError): ไม่สามารถเขียนทับไฟล์ '{output_file_path}' ได้"
    logging.error(error_message)
    logging.error("!!! กรุณาตรวจสอบว่าไฟล์ Excel นี้ถูกเปิดใช้งานค้างไว้ หรือ User ที่รัน Task ไม่มีสิทธิ์เขียนไฟล์ใน Folder ปลายทาง!!!")
except Exception as e:
    logging.error(f"เกิดข้อผิดพลาดที่ไม่คา