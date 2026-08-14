import os
import glob
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import clickhouse_connect
from prefect import task, flow
from prefect.logging import get_run_logger

# ==========================================
# 0. Prefect Environment Setup
# ==========================================
# Point Prefect to Docker Server or Local Ephemeral
os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# ==========================================
# 1. Environment & Database Configuration
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CLEAN_DIR = BASE_DIR / "data" / "clean_zone"
CLEAN_DATA_DIR = Path(os.getenv("CLEAN_DATA_DIR", str(DEFAULT_CLEAN_DIR)))

# ClickHouse HTTP Port is 8123 (Used by clickhouse-connect)
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")) 
CH_DB   = os.getenv("CLICKHOUSE_DB", "omni-db")
CH_USER = os.getenv("CLICKHOUSE_USER", "myuser")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "mypassword")

def get_clickhouse_client():
    # 1. เชื่อมต่อแบบไม่ระบุเป้าหมาย (จะเข้าไปที่ 'default' database อัตโนมัติ)
    init_client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS
    )
    
    # 2. สั่งสร้าง Database ตามชื่อตัวแปร CH_DB หากยังไม่มีอยู่
    init_client.command(f"CREATE DATABASE IF NOT EXISTS `{CH_DB}`")
    
    # 3. ส่งคืน Client ที่เชื่อมต่อไปยัง Database นั้นโดยตรง
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS,
        database=CH_DB
    )

# ==========================================
# 2. Helper Functions
# ==========================================
def load_parquet_table(db_name: str, table_name: str) -> pd.DataFrame:
    target_dir = CLEAN_DATA_DIR / db_name / table_name
    files = list(target_dir.rglob("*.parquet"))
    
    if not files and target_dir.parent.exists():
        for folder in target_dir.parent.iterdir():
            if folder.is_dir() and folder.name.lower() == table_name.lower():
                files = list(folder.rglob("*.parquet"))
                break

    if not files:
        raise FileNotFoundError(
            f"❌ No parquet files found for '{db_name}.{table_name}'. "
            f"Searched path: {target_dir.resolve()}"
        )
    
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

# ==========================================
# 3. Prefect Tasks: Dimension Builders
# ==========================================
@task(name="Build Dim Source System", retries=2, retry_delay_seconds=5)
def build_dim_source_system() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Building dim_source_system...")
    data = [
        {"source_system_id": 1, "source_system_name": "Chinook", "description": "Digital Media Store"},
        {"source_system_id": 2, "source_system_name": "Northwind", "description": "Food and Beverage Distributor"}
    ]
    df = pd.DataFrame(data)
    df["source_system_id"] = df["source_system_id"].astype("int32")
    return df

@task(name="Build Dim Date", retries=2, retry_delay_seconds=5)
def build_dim_date(start_date="2000-01-01", end_date="2030-12-31") -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Building dim_date...")
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"full_date": date_range})
    
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype("int32")
    df["full_date"] = df["full_date"].dt.date
    df["day_of_month"] = pd.DatetimeIndex(df["full_date"]).day.astype("int32")
    df["day_of_week"] = pd.DatetimeIndex(df["full_date"]).day_name()
    df["month"] = pd.DatetimeIndex(df["full_date"]).month.astype("int32")
    df["month_name"] = pd.DatetimeIndex(df["full_date"]).month_name()
    df["quarter"] = pd.DatetimeIndex(df["full_date"]).quarter.astype("int32")
    df["year"] = pd.DatetimeIndex(df["full_date"]).year.astype("int32")
    
    return df[["date_key", "full_date", "day_of_month", "day_of_week", "month", "month_name", "quarter", "year"]]

@task(name="Build Dim Customers", retries=2, retry_delay_seconds=5)
def build_dim_customers() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Building dim_customers...")
    
    ch_cust = load_parquet_table("chinook", "Customer")
    ch_df = pd.DataFrame({
        "customer_id": "CH_" + ch_cust["CustomerId"].astype(str),
        "company_name": ch_cust["Company"].fillna("N/A"),
        "contact_first_name": ch_cust["FirstName"],
        "contact_last_name": ch_cust["LastName"],
        "contact_title": "N/A",
        "address": ch_cust.get("Address", "N/A"),
        "city": ch_cust.get("City", "N/A"),
        "state_region": ch_cust.get("State", "N/A"),
        "postal_code": ch_cust.get("PostalCode", "N/A"),
        "country": ch_cust.get("Country", "N/A"),
        "phone": ch_cust.get("Phone", "N/A"),
        "email": ch_cust.get("Email", "N/A"),
        "support_rep_id": ch_cust["SupportRepId"].apply(lambda x: f"C{int(x):04d}" if pd.notna(x) and str(x).isdigit() else "N/A"),
        "source_system": "Chinook"
    })

    nw_cust = load_parquet_table("northwind", "Customers")
    names = nw_cust["ContactName"].fillna("UNKNOWN").str.split(" ", n=1, expand=True)
    first_names = names[0]
    last_names = names[1].fillna("UNKNOWN") if names.shape[1] > 1 else "UNKNOWN"

    nw_df = pd.DataFrame({
        "customer_id": "NW_" + nw_cust["CustomerID"].astype(str),
        "company_name": nw_cust["CompanyName"],
        "contact_first_name": first_names,
        "contact_last_name": last_names,
        "contact_title": nw_cust.get("ContactTitle", "N/A"),
        "address": nw_cust.get("Address", "N/A"),
        "city": nw_cust.get("City", "N/A"),
        "state_region": nw_cust.get("Region", "N/A"),
        "postal_code": nw_cust.get("PostalCode", "N/A"),
        "country": nw_cust.get("Country", "N/A"),
        "phone": nw_cust.get("Phone", "N/A"),
        "email": "N/A",
        "support_rep_id": "N/A",
        "source_system": "Northwind"
    })
    final_df = pd.concat([ch_df, nw_df], ignore_index=True)
    
    # เติมค่าว่าง (NaN/None) ทั้งหมดใน DataFrame ด้วย "N/A"
    final_df = final_df.fillna("N/A")
    
    return final_df

@task(name="Build Dim Employees", retries=2, retry_delay_seconds=5)
def build_dim_employees() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Building dim_employees...")
    
    ch_emp = load_parquet_table("chinook", "Employee")
    ch_df = pd.DataFrame({
        "employee_id": ch_emp["EmployeeId"].apply(lambda x: f"C{int(x):04d}"),
        "first_name": ch_emp["FirstName"],
        "last_name": ch_emp["LastName"],
        "title": ch_emp.get("Title", "N/A"),
        "title_of_courtesy": "N/A",
        "birth_date": pd.to_datetime(ch_emp.get("BirthDate", None)).dt.date,
        "hire_date": pd.to_datetime(ch_emp.get("HireDate", None)).dt.date,
        "address": ch_emp.get("Address", "N/A"),
        "city": ch_emp.get("City", "N/A"),
        "state_region": ch_emp.get("State", "N/A"),
        "postal_code": ch_emp.get("PostalCode", "N/A"),
        "country": ch_emp.get("Country", "N/A"),
        "phone": ch_emp.get("Phone", "N/A"),
        "extension": "N/A",
        "email": ch_emp.get("Email", "N/A"),
        "reports_to": ch_emp["ReportsTo"].apply(lambda x: f"C{int(x):04d}" if pd.notna(x) and str(x).isdigit() else "N/A"),
        "source_system": "Chinook"
    })

    nw_emp = load_parquet_table("northwind", "Employees")
    nw_df = pd.DataFrame({
        "employee_id": nw_emp["EmployeeID"].apply(lambda x: f"N{int(x):04d}"),
        "first_name": nw_emp["FirstName"],
        "last_name": nw_emp["LastName"],
        "title": nw_emp.get("Title", "N/A"),
        "title_of_courtesy": nw_emp.get("TitleOfCourtesy", "N/A"),
        "birth_date": pd.to_datetime(nw_emp.get("BirthDate", None)).dt.date,
        "hire_date": pd.to_datetime(nw_emp.get("HireDate", None)).dt.date,
        "address": nw_emp.get("Address", "N/A"),
        "city": nw_emp.get("City", "N/A"),
        "state_region": nw_emp.get("Region", "N/A"),
        "postal_code": nw_emp.get("PostalCode", "N/A"),
        "country": nw_emp.get("Country", "N/A"),
        "phone": nw_emp.get("HomePhone", "N/A"),
        "extension": nw_emp.get("Extension", "N/A"),
        "email": "N/A",
        "reports_to": nw_emp["ReportsTo"].apply(lambda x: f"N{int(x):04d}" if pd.notna(x) and str(x).isdigit() else "N/A"),
        "source_system": "Northwind"
    })

    final_df = pd.concat([ch_df, nw_df], ignore_index=True)
    
    # ดึงรายชื่อคอลัมน์ที่เป็นประเภทข้อความ (String/Object)
    string_cols = final_df.select_dtypes(include=['object', 'string']).columns
    # เติมค่าว่างเฉพาะคอลัมน์ที่เป็นข้อความด้วย "N/A"
    final_df[string_cols] = final_df[string_cols].fillna("N/A")
    
    return final_df

@task(name="Build Dim Products", retries=2, retry_delay_seconds=5)
def build_dim_products() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Building dim_products...")
    
    ch_track = load_parquet_table("chinook", "Track")
    ch_df = pd.DataFrame({
        "product_id": "CH_" + ch_track["TrackId"].astype(str),
        "product_name": ch_track["Name"],
        "product_type": "Digital Music Track",
        "category_name": "Music",
        "genre_name": "N/A",
        "composer_supplier": ch_track.get("Composer", "N/A").fillna("N/A"),
        "unit_price": ch_track.get("UnitPrice", 0.99).astype(float)
    })

    nw_prod = load_parquet_table("northwind", "Products")
    nw_df = pd.DataFrame({
        "product_id": "NW_" + nw_prod["ProductID"].astype(str),
        "product_name": nw_prod["ProductName"],
        "product_type": "Food & Beverage Item",
        "category_name": "N/A",
        "genre_name": "N/A",
        "composer_supplier": "N/A",
        "unit_price": nw_prod.get("UnitPrice", 0.0).astype(float)
    })

    return pd.concat([ch_df, nw_df], ignore_index=True)

# ==========================================
# 4. Prefect Task: Fact Table Builder
# ==========================================
@task(name="Build Fact Orders", retries=2, retry_delay_seconds=5)
def build_fact_orders() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Building fact_orders...")
    facts = []

    ch_inv = load_parquet_table("chinook", "Invoice")
    ch_line = load_parquet_table("chinook", "InvoiceLine")
    
    ch_merged = ch_line.merge(
        ch_inv[["InvoiceId", "CustomerId", "InvoiceDate"]], 
        on="InvoiceId", 
        how="inner"
    )

    ch_fact = pd.DataFrame()
    ch_fact["date_key"] = pd.to_datetime(ch_merged["InvoiceDate"]).dt.strftime("%Y%m%d").astype("int32")
    ch_fact["customer_id"] = "CH_" + ch_merged["CustomerId"].astype(str)
    ch_fact["employee_id"] = "N/A"
    ch_fact["product_id"] = "CH_" + ch_merged["TrackId"].astype(str)
    ch_fact["source_system_id"] = np.int32(1)
    ch_fact["source_order_id"] = ch_merged["InvoiceId"].astype(str)
    ch_fact["unit_price"] = ch_merged["UnitPrice"].astype(float)
    ch_fact["quantity"] = ch_merged["Quantity"].astype("int32")
    ch_fact["discount"] = 0.0
    ch_fact["sales_amount"] = (ch_fact["unit_price"] * ch_fact["quantity"]) * (1.0 - ch_fact["discount"])
    facts.append(ch_fact)

    nw_ord = load_parquet_table("northwind", "Orders")
    nw_line = load_parquet_table("northwind", "Order_Details")
    
    nw_merged = nw_line.merge(
        nw_ord[["OrderID", "CustomerID", "EmployeeID", "OrderDate"]], 
        on="OrderID", 
        how="inner"
    )

    nw_fact = pd.DataFrame()
    nw_fact["date_key"] = pd.to_datetime(nw_merged["OrderDate"]).dt.strftime("%Y%m%d").astype("int32")
    nw_fact["customer_id"] = "NW_" + nw_merged["CustomerID"].astype(str)
    nw_fact["employee_id"] = nw_merged["EmployeeID"].apply(lambda x: f"N{int(x):04d}" if pd.notna(x) and str(x).isdigit() else "N/A")
    nw_fact["product_id"] = "NW_" + nw_merged["ProductID"].astype(str)
    nw_fact["source_system_id"] = np.int32(2)
    nw_fact["source_order_id"] = nw_merged["OrderID"].astype(str)
    nw_fact["unit_price"] = nw_merged["UnitPrice"].astype(float)
    nw_fact["quantity"] = nw_merged["Quantity"].astype("int32")
    nw_fact["discount"] = nw_merged.get("Discount", 0.0).astype(float)
    nw_fact["sales_amount"] = (nw_fact["unit_price"] * nw_fact["quantity"]) * (1.0 - nw_fact["discount"])
    facts.append(nw_fact)

    fact_orders = pd.concat(facts, ignore_index=True)
    fact_orders.insert(0, "sales_key", np.arange(10001, 10001 + len(fact_orders), dtype=np.uint64))

    return fact_orders
# ==========================================
# Table Schemas (DDL) for ClickHouse
# ==========================================
DDL_STATEMENTS = {
    "dim_source_system": """
        CREATE TABLE IF NOT EXISTS dim_source_system (
            source_system_id Int32,
            source_system_name String,
            description String
        ) ENGINE = MergeTree() ORDER BY source_system_id
    """,
    "dim_date": """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key Int32,
            full_date Date32,
            day_of_month Int32,
            day_of_week String,
            month Int32,
            month_name String,
            quarter Int32,
            year Int32
        ) ENGINE = MergeTree() ORDER BY date_key
    """,
    "dim_customers": """
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id String,
            company_name String,
            contact_first_name String,
            contact_last_name String,
            contact_title String,
            address String,
            city String,
            state_region String,
            postal_code String,
            country String,
            phone String,
            email String,
            support_rep_id String,
            source_system String
        ) ENGINE = MergeTree() ORDER BY customer_id
    """,
    "dim_employees": """
        CREATE TABLE IF NOT EXISTS dim_employees (
            employee_id String,
            first_name String,
            last_name String,
            title String,
            title_of_courtesy String,
            birth_date Nullable(Date32),
            hire_date Nullable(Date32),
            address String,
            city String,
            state_region String,
            postal_code String,
            country String,
            phone String,
            extension String,
            email String,
            reports_to String,
            source_system String
        ) ENGINE = MergeTree() ORDER BY employee_id
    """,
    "dim_products": """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id String,
            product_name String,
            product_type String,
            category_name String,
            genre_name String,
            composer_supplier String,
            unit_price Float64
        ) ENGINE = MergeTree() ORDER BY product_id
    """,
    "fact_orders": """
        CREATE TABLE IF NOT EXISTS fact_orders (
            sales_key UInt64,
            date_key Int32,
            customer_id String,
            employee_id String,
            product_id String,
            source_system_id Int32,
            source_order_id String,
            unit_price Float64,
            quantity Int32,
            discount Float64,
            sales_amount Float64
        ) ENGINE = MergeTree() ORDER BY sales_key
    """
}
# ==========================================
# 5. Prefect Task: Load to ClickHouse
# ==========================================
@task(name="Load Table to ClickHouse", retries=3, retry_delay_seconds=10)
def load_to_clickhouse(table_name: str, df: pd.DataFrame):
    logger = get_run_logger()
    client = get_clickhouse_client()
    
    logger.info(f"🛠 Preparing table `{table_name}` schema...")
    
    # 1. ลบตารางเดิมทิ้ง (เหมือน if_exists="replace")
    client.command(f"DROP TABLE IF EXISTS `{table_name}`")
    
    # 2. สร้างตารางใหม่ด้วย DDL ที่เรากำหนดไว้
    client.command(DDL_STATEMENTS[table_name])
    
    logger.info(f"⏳ Ingesting {table_name} ({len(df)} records) into ClickHouse...")
    
    # 3. โหลดข้อมูลเข้า ClickHouse
    client.insert_df(
        table=table_name,
        df=df
    )
    logger.info(f"✅ Successfully loaded {table_name} into ClickHouse.")
# ==========================================
# 6. Main Orchestration Flow
# ==========================================
@flow(name="Star Schema ETL Pipeline (ClickHouse)", log_prints=True)
def star_schema_etl_flow():
    print(f"🚀 Starting Pipeline with CLEAN_DATA_DIR: {CLEAN_DATA_DIR.resolve()}")

    dim_source_df = build_dim_source_system()
    dim_date_df = build_dim_date()
    dim_cust_df = build_dim_customers()
    dim_emp_df = build_dim_employees()
    dim_prod_df = build_dim_products()
    fact_orders_df = build_fact_orders()

    tables = {
        "dim_source_system": dim_source_df,
        "dim_date": dim_date_df,
        "dim_customers": dim_cust_df,
        "dim_employees": dim_emp_df,
        "dim_products": dim_prod_df,
        "fact_orders": fact_orders_df
    }

    for table_name, df in tables.items():
        load_to_clickhouse(table_name, df)

    print("\n🎉 Star Schema ETL Pipeline Completed Successfully!")

if __name__ == "__main__":
    star_schema_etl_flow()