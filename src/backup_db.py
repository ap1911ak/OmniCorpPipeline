import os
import subprocess
import datetime
from pathlib import Path
from prefect import flow, task, get_run_logger

# ==========================================
# 🛑 FORCED ENVIRONMENT SETUP
# ==========================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKUP_DIR = PROJECT_ROOT / "data" / "backups"

# ==========================================
# Database Config (อ้างอิงตาม docker-compose)
# ==========================================
CONTAINER_NAME = "postgres_db"
DB_USER = "myuser"
DB_PASS = "mypassword"
DB_NAME = "omni-db"  # เปลี่ยนชื่อ DB ตามที่ต้องการ

@task(name="Run pg_dump via Docker", retries=1, retry_delay_seconds=10)
def run_docker_pg_dump():
    logger = get_run_logger()
    
    # สร้างโฟลเดอร์สำหรับเก็บไฟล์ Backup ถ้ายังไม่มี
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    # ตั้งชื่อไฟล์ Backup ตามวันเวลา
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"backup_{DB_NAME}_{timestamp}.dump"
    backup_file_path = BACKUP_DIR / backup_filename
    
    logger.info(f"Starting database backup for '{DB_NAME}' via Docker...")

    # คำสั่งสั่งรัน pg_dump ผ่าน Docker พร้อมแนบรหัสผ่าน (-e PGPASSWORD) และระบุผู้ใช้ (-U)
    dump_cmd = [
        "docker", "exec", "-i", 
        "-e", f"PGPASSWORD={DB_PASS}", 
        CONTAINER_NAME,
        "pg_dump", "-U", DB_USER, "-F", "c", DB_NAME
    ]

    try:
        # เปิดไฟล์บนเครื่อง Mac เพื่อรอรับข้อมูลที่พ่นออกมาจาก Docker
        with open(backup_file_path, "wb") as f:
            process = subprocess.run(dump_cmd, stdout=f, stderr=subprocess.PIPE)
            
        # ตรวจสอบว่าคำสั่งพังหรือไม่
        if process.returncode != 0:
            error_msg = process.stderr.decode("utf-8")
            logger.error(f"❌ Backup failed: {error_msg}")
            # ลบไฟล์ที่พังทิ้งเพื่อไม่ให้รก
            if os.path.exists(backup_file_path):
                os.remove(backup_file_path)
            raise RuntimeError(f"Docker pg_dump failed: {error_msg}")
            
        logger.info(f"✅ Successfully backed up database to {backup_filename}")
        
    except FileNotFoundError:
        logger.error("❌ Command 'docker' not found. Please ensure Docker Desktop is running.")
        raise
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {e}")
        raise

@flow(name="OmniCorp Database Backup", log_prints=True)
def postgres_backup_flow():
    logger = get_run_logger()
    logger.info("Initiating One-Click Database Backup Flow (Docker Mode)...")
    
    run_docker_pg_dump()
    
    logger.info("🎉 Database Backup Flow Completed Successfully!")

if __name__ == "__main__":
    postgres_backup_flow.serve(name="One-Click-Backup-Deployment", tags=["backup", "database"])