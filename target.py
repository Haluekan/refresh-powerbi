import pandas as pd

# สร้างวันที่ตั้งแต่วันที่ 1 มกราคม 2025 ถึง 31 ธันวาคม 2025
date_range = pd.date_range(start='2025-01-01', end='2025-12-31', freq='D')

# สร้าง DataFrame
df = pd.DataFrame({
    'TargetDate1': date_range,
    'TargetMonth': 200000000
})

# คำนวณ TargetDay โดยหาร TargetMonth ด้วยจำนวนวันในเดือนนั้นๆ
df['TargetDay'] = df['TargetMonth'] / df['TargetDate1'].dt.days_in_month

# บันทึกไฟล์
output_filename = 'Full_Target_Sales_2025.csv'
df.to_csv(output_filename, index=False)

output_filename