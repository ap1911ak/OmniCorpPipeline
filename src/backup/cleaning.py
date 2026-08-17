import os
import sys
import datetime
import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pathlib import Path
from prefect import task, flow, get_run_logger

# Suppress unnecessary warnings for cleaner console output
warnings.filterwarnings('ignore')

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except (ImportError, TypeError, Exception) as e:
    print(f'⚠️ Warning: Could not initialize Altair ({e}). Dashboard generation will be skipped.')
    alt = None

# ==========================================
# Robust Path Resolution (แก้ปัญหา Path)
# ==========================================
try:
    # กรณีรันเป็น Python Script (.py) 
    # __file__ คือ .../OmniCorpPipeline/src/test_cleaning.py
    SCRIPT_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = SCRIPT_DIR.parent
except NameError:
    # Fallback กรณีรันใน Jupyter Notebook
    PROJECT_ROOT = Path.cwd()

# แปลงเป็น String เพื่อให้เข้ากันได้กับ DuckDB และ OS module
RAW_DATA_DIR = str(PROJECT_ROOT / "data" / "rawData")
CLEAN_DATA_DIR = str(PROJECT_ROOT / "data" / "clean_zone")

CHINOOK_DB = str(Path(RAW_DATA_DIR) / "chinook.db")
NORTHWIND_DB = str(Path(RAW_DATA_DIR) / "northwind.db")

RUN_DATE = datetime.datetime.now().strftime('%Y-%m-%d')

# ==========================================
# Monad-based Data Quality Engine
# ==========================================
class Monad:
    __slots__ = ['value', 'input_row', 'output_column', 'status', 'final_value', 'logs', 'stopped', 'step']

    def __init__(self, value, row, col):
        self.value = value
        self.input_row = row
        self.output_column = col
        self.status = 'pending'
        self.final_value = None
        self.logs = []
        self.stopped = False
        self.step = 0

    def _log(self, func, status, details=None):
        role = getattr(func, '_role', 'unknown')
        name = getattr(func, '__name__', 'unknown')
        self.logs.append({
            'row': self.input_row,
            'col': self.output_column,
            'step': self.step,
            'role_type': role,
            'rule_name': name,
            'status': status,
            'value': str(self.value),
            'details': details
        })

    def __or__(self, func):
        if self.stopped:
            self.step += 1
            self._log(func, 'skipped')
            return self
            
        self.step += 1
        role = getattr(func, '_role', 'validator')
        try:
            res = func(self.value)
            if role == 'validator':
                self.value = res
                self.status = 'valid'
                self._log(func, 'passed')
            elif role == 'transformer':
                self.value = res
                self.status = 'success'
                self.final_value = res
                self.stopped = True 
                self._log(func, 'passed')
            elif role == 'fallback' and self.status in ['dirty', 'error']:
                self.value = res
                self.status = 'recovered'
                self.final_value = res
                self.stopped = True
                self._log(func, 'recovered')
        except Exception as e:
            if role == 'validator':
                self.status = 'dirty'
                self.final_value = None
                self.stopped = True
                self._log(func, 'failed', str(e))
            elif role == 'transformer':
                self.status = 'error'
                self.final_value = None
                self.stopped = True
                self._log(func, 'failed', str(e))
        return self


# ==========================================
# Monad Rules & Decorators
# ==========================================
def validator(func):
    func._role = 'validator'
    return func

def transformer(func):
    func._role = 'transformer'
    return func

@validator
def not_null(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        raise ValueError("Value cannot be null or empty")
    return val

@validator
def is_positive(val):
    if float(val) < 0:
        raise ValueError("Value must be non-negative")
    return val

@transformer
def fill_unknown(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    raise ValueError("Value is already valid; skipping transformation")

@transformer
def fill_zero(val):
    if pd.isna(val) or val is None:
        return 0
    raise ValueError("Value is already valid; skipping transformation")


# ==========================================
# Metadata & Schema Specifications
# ==========================================
SCHEMA_CONFIG = {
    "chinook": {
        "Customer": {"CustomerId": [not_null], "FirstName": [not_null], "LastName": [not_null], "Company": [fill_unknown]},
        "Employee": {"EmployeeId": [not_null], "FirstName": [not_null], "LastName": [not_null], "Title": [fill_unknown]},
        "Invoice": {"InvoiceId": [not_null], "CustomerId": [not_null], "Total": [not_null, is_positive]},
        "InvoiceLine": {"InvoiceLineId": [not_null], "InvoiceId": [not_null], "TrackId": [not_null], "UnitPrice": [not_null, is_positive]},
        "Track": {"TrackId": [not_null], "Name": [not_null], "Milliseconds": [is_positive]},
        "Album": {"AlbumId": [not_null], "Title": [not_null], "ArtistId": [not_null]}
    },
    "northwind": {
        "Customers": {"CustomerID": [not_null], "CompanyName": [not_null], "ContactName": [fill_unknown]},
        "Employees": {"EmployeeID": [not_null], "LastName": [not_null], "FirstName": [not_null]},
        "Orders": {"OrderID": [not_null], "CustomerID": [not_null], "EmployeeID": [not_null]},
        "Order Details": {"OrderID": [not_null], "ProductID": [not_null], "UnitPrice": [not_null, is_positive], "Quantity": [not_null, is_positive]},
        "Products": {"ProductID": [not_null], "ProductName": [not_null], "UnitPrice": [is_positive]}
    }
}


# ==========================================
# Prefect Tasks
# ==========================================
@task(name="Extract Data from DuckDB", retries=2, retry_delay_seconds=3)
def extract_data(db_path: str, table_name: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Extracting table '{table_name}' from {db_path}")
    conn = duckdb.connect(db_path)
    try:
        safe_table_name = f'"{table_name}"' if ' ' in table_name else table_name
        df = conn.execute(f"SELECT * FROM {safe_table_name}").df()
        return df
    finally:
        conn.close()

@task(name="Apply Monad Data Quality Rules")
def process_data_quality(df: pd.DataFrame, schema: dict, table_name: str, db_name: str):
    logger = get_run_logger()
    logger.info(f"Applying DQ rules for {db_name}.{table_name}")
    all_logs = []
    df_clean = df.copy()
    
    for col, rules in schema.items():
        if col not in df_clean.columns:
            continue
            
        cleaned_col = []
        for idx, val in enumerate(df_clean[col]):
            m = Monad(val, idx, col)
            for rule in rules:
                m = m | rule
            
            for log in m.logs:
                log['db_name'] = db_name
                log['table_name'] = table_name
                
            all_logs.extend(m.logs)
            final_val = m.final_value if m.final_value is not None else m.value
            cleaned_col.append(final_val)
            
        df_clean[col] = cleaned_col
        
    return df_clean, all_logs

@task(name="Load to Parquet Data Lake")
def load_data(df_clean: pd.DataFrame, db_name: str, table_name: str):
    logger = get_run_logger()
    df_clean['run_date'] = RUN_DATE
    table = pa.Table.from_pandas(df_clean)
    
    dataset_path = os.path.join(CLEAN_DATA_DIR, db_name, table_name.replace(" ", "_"))
    pq.write_to_dataset(
        table,
        root_path=dataset_path,
        partition_cols=['run_date'],
        existing_data_behavior='overwrite_or_ignore'
    )
    logger.info(f"Saved clean data to: {dataset_path}")


# ==========================================
# Prefect Flow (Main Pipeline)
# ==========================================
@flow(name="Daily Data Ingestion Pipeline")
def main_flow():
    logger = get_run_logger()
    logger.info(f"🚀 Starting Pipeline. Resolving paths:")
    logger.info(f"   • Project Root: {PROJECT_ROOT}")
    logger.info(f"   • Raw Data: {RAW_DATA_DIR}")
    
    os.makedirs(CLEAN_DATA_DIR, exist_ok=True)
    
    master_logs = []
    dbs = {"chinook": CHINOOK_DB, "northwind": NORTHWIND_DB}
    
    for db_name, db_path in dbs.items():
        if not os.path.exists(db_path):
            logger.error(f"❌ Database file not found: {db_path}")
            # แจ้งเตือนหากหาโฟลเดอร์ไม่เจอ
            logger.error(f"   โปรดตรวจสอบว่ามีไฟล์ {db_path} อยู่จริงหรือไม่")
            continue
            
        db_schema = SCHEMA_CONFIG.get(db_name, {})
        for table_name, table_schema in db_schema.items():
            try:
                # 1. Extract
                df = extract_data(db_path, table_name)
                
                # 2. Transform / QA
                df_clean, logs = process_data_quality(df, table_schema, table_name, db_name)
                master_logs.extend(logs)
                
                # 3. Load
                load_data(df_clean, db_name, table_name)
                
            except Exception as e:
                logger.error(f"❌ Failed to process {db_name}.{table_name}: {str(e)}")

    
    logger.info("🎉 Pipeline Execution Completed Successfully!")

if __name__ == "__main__":
    main_flow()