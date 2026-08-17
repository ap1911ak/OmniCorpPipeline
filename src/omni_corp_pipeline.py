import os
import sys
import datetime
from pathlib import Path
from prefect import flow, get_run_logger

# ==========================================
# 🛑 FORCED ENVIRONMENT SETUP (แก้ปัญหา Prefect Background Run)
# บังคับให้ Python รู้จักโฟลเดอร์ src และโปรเจกต์เสมอ
# ==========================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# 1. บังคับเพิ่ม src/ เข้าไปในระบบ เพื่อให้ import zone1, zone2 ได้
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# 2. บังคับเปลี่ยน Working Directory กลับมาที่หน้าโปรเจกต์
os.chdir(PROJECT_ROOT)

# จากนั้นค่อย import ไฟล์โซนต่างๆ ตามปกติ
import zone1_raw as z1
import zone2_clean as z2
import zone3_staging as z3
import zone4_mart as z4

# ---------------------------------------------------------
# Configuration Paths & URLs
# ---------------------------------------------------------
RAW_DATA_DIR = PROJECT_ROOT / "data" / "rawData"
CLEAN_DATA_DIR = PROJECT_ROOT / "data" / "clean_zone"
RUN_DATE = datetime.datetime.now().strftime('%Y-%m-%d')

CHINOOK_URL = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
NORTHWIND_URL = "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"

# ---------------------------------------------------------
# Main ETL Flow
# ---------------------------------------------------------
@flow(name="OmniCorp Master Data Pipeline", log_prints=True)
def omnicorp_master_etl():
    logger = get_run_logger()
    logger.info("🚀 Starting OmniCorp Data Pipeline...")
    
    # Setup Directories
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

    # ==========================================
    # ZONE 1: RAW (Extract)
    # ==========================================
    logger.info("--- 🗄️ Entering ZONE 1: RAW ---")
    chinook_db_path = str(RAW_DATA_DIR / "chinook.db")
    northwind_db_path = str(RAW_DATA_DIR / "northwind.db")
    z1.download_file(CHINOOK_URL, chinook_db_path)
    z1.download_file(NORTHWIND_URL, northwind_db_path)

    # ==========================================
    # ZONE 2: CLEAN (Transform & QA)
    # ==========================================
    logger.info("--- 🧹 Entering ZONE 2: CLEAN ---")
    dbs = {"chinook": chinook_db_path, "northwind": northwind_db_path}
    for db_name, db_path in dbs.items():
        db_schema = z2.SCHEMA_CONFIG.get(db_name, {})
        for table_name in db_schema.keys():
            df_raw = z2.extract_data(db_path, table_name)
            df_clean = z2.process_data_quality(df_raw, db_name, table_name)
            z2.load_clean_data(df_clean, str(CLEAN_DATA_DIR), db_name, table_name, RUN_DATE)

# ==========================================
    # ZONE 3: STAGING (Star Schema Build)
    # ==========================================
    logger.info("--- 🏗️ Entering ZONE 3: STAGING ---")
    dim_source = z3.build_dim_source_system()
    dim_date = z3.build_dim_date()
    dim_cust = z3.build_dim_customers(CLEAN_DATA_DIR)
    dim_emp = z3.build_dim_employees(CLEAN_DATA_DIR)    
    dim_prod = z3.build_dim_products(CLEAN_DATA_DIR)
    fact_orders = z3.build_fact_orders(CLEAN_DATA_DIR)

    # ==========================================
    # ZONE 4: MART (Load to Clickhouse)
    # ==========================================
    logger.info("--- 🚀 Entering ZONE 4: MART ---")
    tables_to_load = {
        "dim_source_system": dim_source,
        "dim_date": dim_date,
        "dim_customers": dim_cust,
        "dim_employees": dim_emp,                      
        "dim_products": dim_prod,
        "fact_orders": fact_orders
    }
    
    for table_name, df in tables_to_load.items():
        z4.load_to_clickhouse(table_name, df)

    logger.info("🎉 OmniCorp Master ETL Completed Successfully!")

# ---------------------------------------------------------
# Web Interface Serving Config
# ---------------------------------------------------------
if __name__ == "__main__":
    omnicorp_master_etl.serve(name="Daily-ETL-Deployment", tags=["etl", "omnicorp"])