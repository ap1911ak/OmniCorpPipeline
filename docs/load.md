# Load

`src/pipeline_framework/load.py` มี adapter interface เดียวสำหรับ SQLite, PostgreSQL, ClickHouse และ SQL Server

`full_replace` ลบ target tables แล้วโหลด dimensions ก่อน facts ใน transaction สำหรับ SQLAlchemy sinks. `incremental` upsert dimensions และแทนที่ fact rows ตาม deterministic key

ClickHouse ใช้ synchronous DELETE mutation และไม่มี cross-table transaction หรือ FK enforcement. ติดตั้ง driver ตาม extra ใน `pyproject.toml`

ตัวอย่าง:

```bash
.venv/bin/pipeline-framework configs/chinook.yaml configs/northwind.yaml --sink sqlite --mode full_replace
```
