import pyodbc
import pandas as pd
import logging

# ----------------------------------------------------------------------
# 3. ตั้งค่าการบันทึก Log (เหมือนเดิม)
# ----------------------------------------------------------------------
logging.basicConfig(
    filename='SalesRevenues.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# ----------------------------------------------------------------------
# 1. ตั้งค่าการเชื่อมต่อ (เหมือนเดิม)
# ----------------------------------------------------------------------
connection_string = (
    "DRIVER={SQL Server};"
    "SERVER=sap-temp.datamars.local;"
    "DATABASE=DB_SAP_DMTHAI;"
    "UID=ro;"
    "PWD=DMquery!"
)

# ----------------------------------------------------------------------
# 2. ชื่อ View และไฟล์ (!!! แก้ไขตรงนี้ !!!)
# ----------------------------------------------------------------------
view_name = "vw_SalesRevenues"

# เปลี่ยนจาก .xlsx เป็น .csv
output_csv_file = "SalesRevenues.csv" 

# sql_query (เหมือนเดิม)
# ถ้าข้อมูลเยอะ ให้กรองข้อมูลที่นี่ เช่น เพิ่ม WHERE [DocDate] >= ...
sql_query = f"SELECT * FROM {view_name};"

# ----------------------------------------------------------------------
# 4. ส่วนเชื่อมต่อและดึงข้อมูล (!!! แก้ไขตรงนี้ !!!)
# ----------------------------------------------------------------------
logging.info("--- [!] เริ่มการทำงานของสคริปต์ (โหมด CSV) ---")

try:
    logging.info(f"กำลังเชื่อมต่อกับฐานข้อมูล {connection_string.split(';')[1]}...")
    
    with pyodbc.connect(connection_string) as connection:
        logging.info("เชื่อมต่อสำเร็จ!")
        logging.info(f"กำลังดึงข้อมูลจาก View: {view_name}...")
        
        df = pd.read_sql_query(sql_query, connection)
        
        if df.empty:
            logging.warning(f"ไม่พบข้อมูลใน View '{view_name}' (พบบข้อมูล 0 แถว)")
        else:
            logging.info(f"พบข้อมูลทั้งหมด {len(df)} แถว. กำลังเซฟเป็น CSV...")
            
            # --- นี่คือส่วนที่เปลี่ยน ---
            # เปลี่ยนจาก to_excel เป็น to_csv
            # encoding='utf-8-sig' สำคัญมาก! เพื่อให้ Excel เปิดไฟล์ CSV ที่มีภาษาไทยได้ถูกต้อง
            df.to_csv(output_csv_file, index=False, encoding='utf-8-sig') 
            # ---------------------------
            
            logging.info(f"สำเร็จ! บันทึกข้อมูลลงในไฟล์ '{output_csv_file}' เรียบร้อย")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    error_message = f"เกิดข้อผิดพลาด pyodbc: {ex}"
    if sqlstate == '28000':
        error_message = "เกิดข้อผิดพลาด: Login failed. ตรวจสอบ Username/Password"
    elif sqlstate == '08001':
        error_message = "เกิดข้อผิดพลาด: ไม่สามารถเชื่อมต่อ Server ได้. ตรวจสอบชื่อ Server, Port, หรือ Firewall"
    elif sqlstate == '42S02':
        error_message = f"เกิดข้อผิดพลาด: ไม่พบ View '{view_name}'."
        
    logging.error(error_message)

except pd.errors.DatabaseError as e:
    logging.error(f"เกิดข้อผิดพลาดจาก Pandas (DatabaseError): {e}")

except PermissionError:
    # Error นี้ยังเกิดขึ้นได้ ถ้าคุณเปิดไฟล์ .csv ค้างไว้
    error_message = f"เกิดข้อผิดพลาด (PermissionError): ไม่สามารถเขียนทับไฟล์ '{output_csv_file}' ได้"
    logging.error(error_message)
    logging.error("!!! กรุณาตรวจสอบว่าไฟล์ CSV นี้ถูกเปิดใช้งานค้างไว้หรือไม่? !!!")

except Exception as e:
    logging.error(f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {e}", exc_info=True)

finally:
    logging.info("--- [!] จบการทำงานของสคริปต์ --- \n")