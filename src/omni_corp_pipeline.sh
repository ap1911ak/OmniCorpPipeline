#!/bin/bash
cd "$(dirname "$0")" || exit

# บังคับให้ Prefect Client ใน Python รู้ว่า Server ทำงานอยู่ที่ Port 4200
export PREFECT_API_URL="http://127.0.0.1:4200/api"

echo "🚀 Starting OmniCorp One-Click Pipeline Deployment..."
mkdir -p ../data/rawData ../data/clean_zone ../data/backups

echo "📦 Deploying Master ETL Flow to Prefect Server..."
# รัน background process และเก็บ log ไว้ตรวจสอบ
nohup python omni_corp_pipeline.py > etl_deployment.log 2>&1 &

echo "📦 Deploying Backup DB Flow to Prefect Server..."
# รัน background process และเก็บ log ไว้ตรวจสอบ
nohup python backup_db.py > backup_deployment.log 2>&1 &

echo "📦 Deploying Restore Metabase Flow to Prefect Server..."
# รัน background process และเก็บ log ไว้ตรวจสอบ
nohup python restore_metabase.py > restore_deployment.log 2>&1 &

echo "✅ Deployments are running in the background."
echo "🔍 หากไม่พบ Flow บน UI ให้ลองตรวจสอบ Log โดยพิมพ์: cat etl_deployment.log หรือ cat restore_deployment.log"
echo "🌐 You can now trigger flows directly from the Prefect Web UI: http://127.0.0.1:4200"