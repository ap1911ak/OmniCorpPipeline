# Integration

`src/pipeline_framework/integration.py` อ่าน valid Staging Parquet แล้วเขียนสำเนาไป `data/integration/`

Integration ห้ามแก้ค่าข้อมูลหรือ column order. ขั้นตอนตรวจ row count, schema และ value checksum ก่อนเขียน

Custom keys `monad_results.*` ถูกลบออก. ชื่อไฟล์ใช้ `i.<source>.<table>.<HHMM>.parquet`
