from prefect import flow, task
import subprocess
import os
import glob

# 1. หาตำแหน่ง Absolute Path ของโฟลเดอร์โปรเจกต์ (ถอยหลังกลับไป 1 ชั้นจากโฟลเดอร์ src)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 2. นำมาต่อกับ Path ของไฟล์ Backup
BACKUP_DIR = os.path.join(BASE_DIR, "data", "backups")

# 2. ค้นหาไฟล์ทั้งหมดที่ลงท้ายด้วย .dump ในโฟลเดอร์นั้น
# (ใช้ glob.glob ช่วยกรองนามสกุลไฟล์ได้แม่นยำ)
dump_files = glob.glob(os.path.join(BACKUP_DIR, "*.dump"))

if dump_files:
    # 3. หาไฟล์ล่าสุดโดยเทียบจากเวลาแก้ไข (Modification Time)
    BACKUP_FILE = max(dump_files, key=os.path.getmtime)
    print(f"พบไฟล์สำรองล่าสุด: {BACKUP_FILE}")
else:
    # 4. เผื่อกรณีไม่มีไฟล์ในโฟลเดอร์เลย
    BACKUP_FILE = None
    print("แจ้งเตือน: ไม่พบไฟล์สำรอง (.dump) ในโฟลเดอร์")

    
DB_CONTAINER = "postgres_db"
SERVICE_NAME = "metabase"

@task
def stop_container(service_name: str):
    print(f"Stopping {service_name}...")
    subprocess.run(["docker", "compose", "stop", service_name], check=True)

@task
def copy_backup_to_container(local_path: str, container_name: str):
    print(f"Copying {local_path} to {container_name}...")
    dest = f"{container_name}:/tmp/metabase_backup.dump"
    
    # เพิ่มการดักจับ Error เพื่อให้เห็นข้อความแจ้งเตือนที่ชัดเจนขึ้นหากเกิดปัญหาอีก
    result = subprocess.run(["docker", "cp", local_path, dest], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Docker CP failed: {result.stderr}")
    print("Copy successful!")

@task
def restore_database(container_name: str):
    print("Starting restore process...")
    cmd = [
        "docker", "exec", "-t", container_name, 
        "pg_restore", "-U", "myuser", "-d", "omni-db", "-1", "-c", "--if-exists", 
        "/tmp/metabase_backup.dump"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Restore failed: {result.stderr}")
    print("Restore successful!")

@task
def start_container(service_name: str):
    print(f"Starting {service_name}...")
    subprocess.run(["docker", "compose", "start", service_name], check=True)

@flow(name="Metabase Restore Flow")
def metabase_restore_flow():
    stop_container(SERVICE_NAME)
    copy_backup_to_container(BACKUP_FILE, DB_CONTAINER)
    restore_database(DB_CONTAINER)
    start_container(SERVICE_NAME)

if __name__ == "__main__":
    metabase_restore_flow.serve(name="Restore-Metabase-Dashboard", tags=["omnicorp","restore","metabse" ])