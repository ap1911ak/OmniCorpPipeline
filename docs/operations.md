# Operations

ทุก run สร้าง `manifests/<run_id>.json` เก็บ status, input fingerprint, output checksum, row counts, schema hash, rejected rows และ output paths

ใช้ `run_id` เดิมเพื่อคืนผลสำเร็จเดิม. ไฟล์เดิมที่ข้อมูลเหมือนกันถูก reuse; ข้อมูลต่างกันใน partition เดิมทำให้เกิด collision และไม่เขียนทับเงียบ

ถ้า run หยุดหลัง Integration ให้ rerun ด้วย `run_id` เดิมเพื่อ resume Load. Schema drift เทียบกับ successful run ก่อนหน้าของ source และหยุดก่อน Landing

ตรวจระบบ:

```bash
.venv/bin/pytest -q
```
