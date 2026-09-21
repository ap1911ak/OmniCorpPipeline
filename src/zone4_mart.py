import os
import pandas as pd
import clickhouse_connect
from prefect import task, get_run_logger

# 🛑 อัปเดต DDL ให้ตรงกับ Metadata ของ Star Schema ล่าสุด (ครบทุกคอลัมน์)
DDL_STATEMENTS = {
    "dim_source_system": "CREATE TABLE IF NOT EXISTS dim_source_system (source_system_id Int32, source_system_name String, description String) ENGINE = ReplacingMergeTree() ORDER BY source_system_id",
    
    "dim_date": "CREATE TABLE IF NOT EXISTS dim_date (date_key Int32, full_date Date32, day_of_month Int32, day_of_week String, month Int32, month_name String, quarter Int32, year Int32) ENGINE = ReplacingMergeTree() ORDER BY date_key",
    
    "dim_customers": "CREATE TABLE IF NOT EXISTS dim_customers (customer_id String, company_name String, contact_first_name String, contact_last_name String, contact_title String, address String, city String, state_region String, postal_code String, country String, phone String, email String, support_rep_id String, source_system String) ENGINE = ReplacingMergeTree() ORDER BY customer_id",
    
    "dim_employees": "CREATE TABLE IF NOT EXISTS dim_employees (employee_id String, first_name String, last_name String, title String, title_of_courtesy String, birth_date Date32, hire_date Date32, address String, city String, state_region String, postal_code String, country String, phone String, extension String, email String, reports_to String, source_system String) ENGINE = ReplacingMergeTree() ORDER BY employee_id",
    
    "dim_products": "CREATE TABLE IF NOT EXISTS dim_products (product_id String, product_name String, product_type String, category_name String, genre_name String, composer_supplier String, unit_price Float64) ENGINE = ReplacingMergeTree() ORDER BY product_id",
    
    "fact_orders": "CREATE TABLE IF NOT EXISTS fact_orders (sales_key UInt64, date_key Int32, customer_id String, employee_id String, product_id String, source_system_id Int32, source_order_id String, unit_price Float64, quantity Int32, discount Float64, sales_amount Float64) ENGINE = ReplacingMergeTree() ORDER BY sales_key"
}

def get_clickhouse_client():
    # บังคับใช้ 127.0.0.1 แทน localhost เพื่อป้องกัน Error [Errno 61]
    CH_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
    CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")) 
    CH_DB   = os.getenv("CLICKHOUSE_DB", "omni-db")
    CH_USER = os.getenv("CLICKHOUSE_USER", "myuser")
    CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "mypassword")

    init_client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS)
    init_client.command(f"CREATE DATABASE IF NOT EXISTS `{CH_DB}`")
    
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB)

@task(name="Zone 4: Load Table to ClickHouse", retries=3, retry_delay_seconds=10)
def load_to_clickhouse(table_name: str, df: pd.DataFrame):
    logger = get_run_logger()
    client = get_clickhouse_client()
    
    logger.info(f"🛠 Preparing table `{table_name}` schema...")
    
    
    client.command(DDL_STATEMENTS[table_name])
    
    logger.info(f"⏳ Upserting {table_name} ({len(df)} records) into ClickHouse...")
    client.insert_df(table=table_name, df=df)
    
    logger.info(f"🧹 Optimizing {table_name} to remove duplicated records...")
    client.command(f"OPTIMIZE TABLE `{table_name}` FINAL")
    
    logger.info(f"✅ Successfully loaded {table_name} into ClickHouse.")