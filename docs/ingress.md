# Ingress

`src/pipeline_framework/ingress.py` อ่าน local file, URL, cloud URI, API และ HTML table

รองรับ SQLite, CSV, Excel, JSON, JSON Lines และ Parquet. Excel อ่านหลาย sheet และ melt pivot/cross-tab เป็น long form ได้

ทุก source เขียน `data/ingress/<source>/<run_id>/<alias>.parquet` พร้อม metadata `_ingested_at`, `_source_uri`, `_source_file`, `_source_sheet` และ `_run_id`

SQLite ใช้ read-only connection. API ใช้ retry, timeout, pagination และ `max_pages`. Cloud ต้องติดตั้ง extra `cloud`

รัน profile อย่างเดียว:

```bash
.venv/bin/pipeline-framework configs/chinook.yaml --profile-only
```
