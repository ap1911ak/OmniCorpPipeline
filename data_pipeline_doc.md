# OmniCorp Data Pipeline

คู่มือนี้อธิบายวิธีติดตั้งและรัน OmniCorp Data Pipeline หลัง clone repository

## 1. ภาพรวม

Pipeline มี 4 zone:

1. **RAW** ดาวน์โหลดฐานข้อมูล Chinook และ Northwind เก็บใน data/rawData
2. **CLEAN** ตรวจสอบ data quality และบันทึก Parquet ใน data/clean_zone
3. **STAGING** สร้าง dimension และ fact สำหรับ star schema
4. **MART** สร้างตารางและโหลดข้อมูลเข้า ClickHouse

ระบบหลัก: Python 3.11, Docker, Docker Compose, PostgreSQL, ClickHouse, Prefect Server, Metabase, CloudBeaver และ JupyterLab

## 2. ข้อกำหนด

ติดตั้ง Git และ Docker Desktop ก่อนเริ่มงาน

~~~bash
git --version
docker --version
docker compose version
~~~

เปิด Docker Desktop ระหว่างใช้งานระบบ

## 3. Clone repository

~~~bash
git clone <REPOSITORY_URL>
cd OmniCorpPipeline
~~~

เปลี่ยน <REPOSITORY_URL> เป็น URL จริงของ repository

## 4. เตรียมไฟล์ .env

สร้างไฟล์ที่ root ของ project:

~~~bash
touch .env
~~~

ใส่ค่าที่ผู้ดูแลระบบส่งให้ภายหลัง ตัวอย่างโครงสร้าง:

~~~dotenv
DB_NAME=omni-db
DB_USER=myuser
DB_PASSWORD=mypassword

CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_DB=omni-db
CLICKHOUSE_USER=myuser
CLICKHOUSE_PASSWORD=mypassword

PREFECT_API_URL=http://prefect-server:4200/api
~~~

ห้าม commit .env ถ้ามี password หรือ secret จริง

> หมายเหตุ: docker-compose.yml ปัจจุบันมีค่ารูปแบบ {DB_NAME}, {DB_USER} และ {DB_PASSWORD} แบบข้อความตรง ๆ ค่าจาก .env จะไม่ถูกนำมาใช้จนกว่าจะเปลี่ยนเป็น ${DB_NAME}, ${DB_USER} และ ${DB_PASSWORD}

## 5. เริ่มระบบ

จาก root ของ project:

~~~bash
docker compose up -d --build
docker compose ps
~~~

ดู log:

~~~bash
docker compose logs -f
~~~

หยุดระบบ:

~~~bash
docker compose down
~~~

คำสั่งนี้ไม่ลบ named volumes

## 6. URL ของระบบ

| ระบบ | URL |
|---|---|
| JupyterLab | http://localhost:8888 |
| Prefect UI | http://localhost:4200 |
| Metabase | http://localhost:3000 |
| CloudBeaver | http://localhost:8978 |
| ClickHouse HTTP | http://localhost:8123 |

ถ้าเปิดไม่ได้ ตรวจสอบ:

~~~bash
docker compose ps
docker compose logs prefect-server
docker compose logs clickhouse
docker compose logs db
~~~

## 7. Deploy Prefect flows

เข้า application container:

~~~bash
docker compose exec app bash
cd /app/src
bash omni_corp_pipeline.sh
~~~

Script จะสร้างโฟลเดอร์ข้อมูลและ deploy flow แบบ background:

- Daily-ETL-Deployment
- One-Click-Backup-Deployment
- Restore-Metabase-Dashboard

ตรวจสอบ log:

~~~bash
cat etl_deployment.log
cat backup_deployment.log
cat restore_deployment.log
~~~

เปิด Prefect UI ที่ http://localhost:4200 แล้วตรวจสอบ deployment

## 8. Run ETL

ใน Prefect UI เลือก Daily-ETL-Deployment แล้วกด Run

Pipeline ทำงานตามลำดับ:

1. ดาวน์โหลด chinook.db และ northwind.db
2. อ่านข้อมูลจาก SQLite
3. ตรวจสอบ data quality
4. เขียนข้อมูล clean เป็น Parquet
5. สร้าง dimension และ fact tables
6. สร้างตารางใน ClickHouse
7. โหลดข้อมูลเข้า ClickHouse

ตารางหลักใน ClickHouse:

~~~text
dim_source_system
dim_date
dim_customers
dim_employees
dim_products
fact_orders
~~~

## 9. ตรวจสอบผลลัพธ์

ดูไฟล์ข้อมูล:

~~~bash
find data -maxdepth 3 -type f | sort
~~~

ตัวอย่าง SQL ใน CloudBeaver หรือ ClickHouse client:

~~~sql
SHOW TABLES;

SELECT count(*) FROM dim_customers;
SELECT count(*) FROM dim_employees;
SELECT count(*) FROM dim_products;
SELECT count(*) FROM fact_orders;
~~~

ดู log ETL:

~~~bash
docker compose exec app bash
cd /app/src
tail -f etl_deployment.log
~~~

## 10. Backup PostgreSQL

ใน Prefect UI เลือก One-Click-Backup-Deployment แล้วกด Run

ไฟล์ backup อยู่ใน data/backups/ และมีรูปแบบ:

~~~text
backup_omni-db_YYYYMMDD_HHMMSS.dump
~~~

Flow นี้เรียกคำสั่ง docker และ container postgres_db หาก flow รันใน container แล้วพบว่าไม่พบคำสั่ง docker ให้รัน backup จาก host หรือปรับ Docker socket และสิทธิ์ของ container

## 11. Restore Metabase

ตรวจสอบว่ามีไฟล์ backup:

~~~bash
find data/backups -name '*.dump' -type f -print
~~~

ใน Prefect UI เลือก Restore-Metabase-Dashboard แล้วกด Run

Flow จะเลือกไฟล์ .dump ล่าสุด แล้ว:

1. หยุด service metabase
2. คัดลอก backup เข้า postgres_db
3. Restore ด้วย pg_restore
4. เริ่ม service metabase

ตรวจสอบ Metabase ที่ http://localhost:3000

## 12. คำสั่งที่ใช้บ่อย

~~~bash
docker compose ps
docker compose start
docker compose stop
docker compose restart app
docker compose build --no-cache
docker compose exec app bash
~~~

## 13. Troubleshooting

### Port ถูกใช้งาน

~~~bash
docker compose ps
docker ps
~~~

หยุด process ที่ใช้ port หรือแก้ port mapping ใน docker-compose.yml

### ClickHouse เชื่อมต่อไม่ได้

~~~bash
docker compose ps clickhouse
docker compose logs clickhouse
~~~

จาก app ใช้ host clickhouse และ HTTP port 8123

จาก host ใช้ 127.0.0.1:8123

### Prefect ไม่พบ deployment

ตรวจสอบ PREFECT_API_URL:

- ภายใน app: http://prefect-server:4200/api
- จาก host: http://127.0.0.1:4200/api

Deploy ใหม่:

~~~bash
docker compose exec app bash
cd /app/src
bash omni_corp_pipeline.sh
~~~

### ETL ดาวน์โหลดข้อมูลไม่ได้

Zone 1 ต้องใช้อินเทอร์เน็ต ตรวจสอบ network และ URL ใน src/omni_corp_pipeline.py:

- CHINOOK_URL
- NORTHWIND_URL

### ไม่พบข้อมูลใน data

ตรวจสอบ ETL run และ log:

~~~bash
docker compose exec app bash
cd /app/src
tail -n 100 etl_deployment.log
~~~

### vQuery start ไม่ได้

vquery_backend และ vquery_frontend ใช้ pull_policy: never จึงต้องมี image ในเครื่อง:

~~~text
vquery/backend:latest
service-vquery-frontend:1
~~~

ถ้าไม่ได้ใช้ vQuery ให้เอา service นี้ออกจาก compose ชั่วคราว หรือเตรียม image ตามขั้นตอนของทีม

### .env ไม่ส่งค่าเข้า container

ตรวจสอบว่า docker-compose.yml ใช้รูปแบบนี้:

~~~yaml
${DB_NAME}
${DB_USER}
${DB_PASSWORD}
~~~

ไม่ใช่รูปแบบนี้:

~~~yaml
{DB_NAME}
{DB_USER}
{DB_PASSWORD}
~~~

หลังแก้ให้สร้าง service ใหม่:

~~~bash
docker compose up -d --force-recreate
~~~

## 14. โครงสร้าง project

~~~text
.
├── data/
│   ├── backups/
│   ├── clean_zone/
│   └── rawData/
├── src/
│   ├── backup_db.py
│   ├── omni_corp_pipeline.py
│   ├── omni_corp_pipeline.sh
│   ├── restore_metabase.py
│   ├── zone1_raw.py
│   ├── zone2_clean.py
│   ├── zone3_staging.py
│   └── zone4_mart.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── data_pipeline_doc.md
~~~

## 15. เริ่มระบบแบบสั้น

~~~bash
git clone <REPOSITORY_URL>
cd OmniCorpPipeline
touch .env
docker compose up -d --build
docker compose exec app bash
cd /app/src
bash omni_corp_pipeline.sh
~~~

จากนั้นเปิด Prefect UI และ run Daily-ETL-Deployment

## 16. ลบระบบ

หยุดและลบ containers กับ network:

~~~bash
docker compose down
~~~

ลบ containers, network และข้อมูลใน named volumes:

~~~bash
docker compose down -v
~~~

คำสั่ง docker compose down -v ลบข้อมูล PostgreSQL, ClickHouse และ CloudBeaver อย่างถาวร ตรวจสอบ backup ก่อนใช้คำสั่งนี้

