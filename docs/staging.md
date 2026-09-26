# Staging

`src/pipeline_framework/staging.py` ใช้ `monadsquishy.py`, `rules.py` และ `validate.py` สำหรับ cleaning, naming, standardization และ governance

แถวที่ rule ไม่ผ่านแยกไป `data/staging/bad_data/<table>/...` พร้อม `_run_id`, `_failed_column`, `_failed_rule`, `_error_code`, `_error_message` และ `_rejected_at`

Dimension ถูกตรวจและเก็บ accepted keys ก่อนตรวจ fact. Fact ที่อ้างถึง dimension ซึ่งถูก reject จะถูก quarantine ด้วย rule `staged_foreign_key`

Valid Parquet มี footer keys `monad_results.zstd`, `monad_results.codec` และ `monad_results.raw_size`. JSON เป็น minified, บีบอัด Zstandard, sample จำกัด 50 ค่า
