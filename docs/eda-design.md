# EDA and Star Schema Design

`src/pipeline_framework/profiling.py` สร้าง EDA report และ draft DBML จาก Ingress Parquet

รายงานมี row count, type, null rate, distinct count, duplicate count, candidate primary key, candidate relationship และ sensitive-data flag

Pipeline ต้องอ่าน design ที่มี `approval.status: approved` และ DBML ที่มีเนื้อหาเท่านั้น. Design ที่ยังเป็น `draft` หยุดก่อนประมวลผล

แก้ mapping ใน `configs/*.yaml` และ schema ใน `schemas/omnicorp.dbml` แล้ว review ใน Git ก่อนเปลี่ยนสถานะเป็น `approved`
