import pyodbc
import pandas as pd
import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq

# ----------------------------------------------------------------------
# 2. ตั้งค่า Path
# ----------------------------------------------------------------------
log_file_path = r"C:\Users\adm-haluekan.ta\Desktop\pa_log.log"
output_file_path = r"C:\Users\adm-haluekan.ta\Desktop\sales_revenues_output.parquet"

# ----------------------------------------------------------------------
# 3. ตั้งค่า Log
# ----------------------------------------------------------------------
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# ----------------------------------------------------------------------
# 1. เชื่อมต่อ (บังคับใช้ Driver 17 + Timeout)
# ----------------------------------------------------------------------
connection_string = (
    # ต้องติดตั้ง ODBC Driver 17 บน VM ก่อนนะครับ
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
# 4. เริ่มทำงาน (โหมด Parquet + Chunking)
# ----------------------------------------------------------------------
logging.info(f"--- [!] เริ่มการทำงาน (โหมด Ultimate: Parquet + Chunking) ---")

try:
    logging.info(f"กำลังเชื่อมต่อกับฐานข้อมูล...")
    with pyodbc.connect(connection_string) as connection:
        logging.info("เชื่อมต่อสำเร็จ! เริ่มทยอยดึงข้อมูล...")
        
        # กำหนดขนาดก้อน (50,000 แถวต่อครั้ง)
        chunk_size = 50000
        
        # สร้างตัวดึงข้อมูลแบบ Iterator
        data_iterator = pd.read_sql_query(sql_query, connection, chunksize=chunk_size)
        
        writer = None
        total_rows = 0
        
        for i, chunk in enumerate(data_iterator):
            # แปลง Chunk (Pandas) เป็น Table (PyArrow) เพื่อเตรียมเขียน Parquet
            table = pa.Table.from_pandas(chunk)
            
            # ถ้าเป็นรอบแรก ให้สร้างไฟล์และตัวเขียน (Writer)
            if writer is None:
                writer = pq.ParquetWriter(output_file_path, table.schema, compression='snappy')
            
            # เขียนข้อมูลก้อนนี้ลงไปในไฟล์
            writer.write_table(table)
            
            total_rows += len(chunk)
            logging.info(f"   ...บันทึกก้อนที่ {i+1} เรียบร้อย ({len(chunk)} แถว) | รวม {total_rows}")
        
        # ปิดตัวเขียนเมื่อเสร็จสิ้น
        if writer:
            writer.close()
            logging.info(f"สำเร็จ! บันทึกไฟล์ Parquet ทั้งหมด {total_rows} แถว เรียบร้อย")
        else:
            logging.warning("ไม่พบข้อมูล (0 แถว)")

except pyodbc.Error as ex:
    logging.error(f"เกิดข้อผิดพลาด SQL/Driver: {ex}")
    if 'HY000' in str(ex):
        logging.error("!!! Protocol Error: กรุณาตรวจสอบว่าติดตั้ง 'ODBC Driver 17' บน VM หรือยัง")
except ImportError:
    logging.error("เกิดข้อผิดพลาด: ขาด Library. รัน 'pip install pyarrow' ก่อนครับ")
except Exception as e:
    logging.error(f"เกิดข้อผิดพลาด: {e}", exc_info=True)
finally:
    logging.info("--- [!] จบการทำงาน --- \n")