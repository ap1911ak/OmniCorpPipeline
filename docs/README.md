# Pipeline Framework Documentation

เอกสารแบ่งตาม stage ของ batch pipeline:

- [Ingress](ingress.md): อ่าน source หลายรูปแบบและเขียน canonical Parquet
- [EDA and Design](eda-design.md): profile ข้อมูลและอนุมัติ YAML/DBML
- [Landing](landing.md): แยก dimension/fact และสร้าง deterministic keys
- [Staging](staging.md): clean, validate, quarantine และเขียน quality footer
- [Integration](integration.md): ตรวจความเท่าเดิมและลบ custom footer
- [Load](load.md): โหลดเข้า SQLite, PostgreSQL, ClickHouse หรือ SQL Server
- [Operations](operations.md): run, resume, idempotency และตรวจสอบผล

เอกสารใช้ภาษาไทย คงชื่อ module, API และ CLI เป็นภาษาอังกฤษ
