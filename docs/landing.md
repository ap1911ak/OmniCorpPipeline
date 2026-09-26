# Landing

`src/pipeline_framework/landing.py` อ่าน mapping ที่ approved แล้วเขียน dimension ก่อน fact

Dimension ใช้ deterministic source-qualified key จาก source, entity และ natural key. Rerun ข้อมูลเดิมจึงได้ key เดิม

Fact ได้ foreign-key columns ตั้งแต่ Landing. ถ้าหา dimension ไม่พบ แถวถูกเขียน `data/landing/bad_data/` พร้อมเหตุผล `orphan_foreign_key`

Partition ใช้ `year=YYYY/month=MM/day=DD`. Fact ใช้ business date; dimension ใช้ snapshot date. ชื่อไฟล์รูปแบบ `l.<source>.<table>.<HHMM>.parquet`
