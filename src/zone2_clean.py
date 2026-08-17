import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from prefect import task, get_run_logger

# ==========================================
# Data Quality Monad Engine 
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
        self.logs.append({
            'row': self.input_row, 'col': self.output_column, 'step': self.step,
            'role_type': getattr(func, '_role', 'unknown'),
            'rule_name': getattr(func, '__name__', 'unknown'),
            'status': status, 'value': str(self.value), 'details': details
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
            elif role in ['transformer', 'fallback']:
                self.value = res
                self.status = 'success' if role == 'transformer' else 'recovered'
                self.final_value = res
                self.stopped = True
                self._log(func, self.status)
        except Exception as e:
            self.status = 'error' if role == 'transformer' else 'dirty'
            self.final_value = None
            self.stopped = True
            self._log(func, 'failed', str(e))
        return self

def validator(func): 
    func._role = 'validator'
    return func

def transformer(func): 
    func._role = 'transformer'
    return func

@validator
def not_null(val):
    if pd.isna(val) or val is None or str(val).strip() == '': 
        raise ValueError("Value cannot be null")
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
    raise ValueError("Valid value")

# ==========================================
# Schema Config (อัปเดตเพิ่มคอลัมน์ของ Customers และ Employees ตาม Metadata)
# ==========================================
SCHEMA_CONFIG = {
    "chinook": {
        "Customer": {
            "CustomerId": [not_null], "FirstName": [not_null], "LastName": [not_null], "Company": [fill_unknown],
            "Address": [], "City": [], "State": [], "Country": [], "PostalCode": [], "Phone": [], "Email": [], "SupportRepId": []
        },
        "Employee": {
            "EmployeeId": [not_null], "FirstName": [not_null], "LastName": [not_null], "Title": [fill_unknown],
            "ReportsTo": [], "BirthDate": [], "HireDate": [], "Address": [], "City": [], "State": [], 
            "Country": [], "PostalCode": [], "Phone": [], "Email": []
        },
        "Invoice": {"InvoiceId": [not_null], "CustomerId": [not_null], "Total": [not_null, is_positive]},
        "InvoiceLine": {"InvoiceLineId": [not_null], "InvoiceId": [not_null], "TrackId": [not_null], "UnitPrice": [not_null, is_positive]},
        "Track": {"TrackId": [not_null], "Name": [not_null], "Milliseconds": [is_positive]},
        "Genre": {"GenreId": [not_null], "Name": [not_null]}
    },
    "northwind": {
        "Customers": {
            "CustomerID": [not_null], "CompanyName": [not_null], "ContactName": [fill_unknown], "ContactTitle": [],
            "Address": [], "City": [], "Region": [], "PostalCode": [], "Country": [], "Phone": []
        },
        "Employees": {
            "EmployeeID": [not_null], "LastName": [not_null], "FirstName": [not_null], "Title": [],
            "TitleOfCourtesy": [], "BirthDate": [], "HireDate": [], "Address": [], "City": [], 
            "Region": [], "PostalCode": [], "Country": [], "HomePhone": [], "Extension": [], "ReportsTo": []
        },
        "Orders": {"OrderID": [not_null], "CustomerID": [not_null], "EmployeeID": [not_null]},
        "Order Details": {"OrderID": [not_null], "ProductID": [not_null], "UnitPrice": [not_null, is_positive], "Quantity": [not_null, is_positive]},
        "Products": {"ProductID": [not_null], "ProductName": [not_null], "UnitPrice": [is_positive]},
        "Categories": {"CategoryID": [not_null], "CategoryName": [not_null]},
        "Suppliers": {"SupplierID": [not_null], "CompanyName": [not_null]}
    }
}

# ==========================================
# Prefect Tasks
# ==========================================
@task(name="Zone 2: Extract from Source DB (DuckDB)", retries=2)
def extract_data(db_path: str, table_name: str) -> pd.DataFrame:
    """Extracts a table from an SQLite file using DuckDB."""
    logger = get_run_logger()
    logger.info(f"Extracting table '{table_name}' from {db_path}")
    conn = duckdb.connect(db_path)
    try:
        safe_table_name = f'"{table_name}"' if ' ' in table_name else table_name
        return conn.execute(f"SELECT * FROM {safe_table_name}").df()
    finally:
        conn.close()

@task(name="Zone 2: Apply Monad Data Quality")
def process_data_quality(df: pd.DataFrame, db_name: str, table_name: str) -> pd.DataFrame:
    """Applies Data Quality rules defined in SCHEMA_CONFIG."""
    logger = get_run_logger()
    schema = SCHEMA_CONFIG.get(db_name, {}).get(table_name, {})
    
    if not schema:
        logger.info(f"No QA rules for {db_name}.{table_name}, passing raw data.")
        return df

    logger.info(f"Applying DQ rules for {db_name}.{table_name}")
    df_clean = df.copy()
    
    for col, rules in schema.items():
        if col not in df_clean.columns: continue
        cleaned_col = []
        for idx, val in enumerate(df_clean[col]):
            m = Monad(val, idx, col)
            for rule in rules: m = m | rule
            final_val = m.final_value if m.final_value is not None else m.value
            cleaned_col.append(final_val)
        df_clean[col] = cleaned_col
        
    return df_clean

@task(name="Zone 2: Load to Clean Parquet Data Lake")
def load_clean_data(df_clean: pd.DataFrame, clean_dir: str, db_name: str, table_name: str, run_date: str):
    """Saves the cleaned DataFrame as a Parquet file partitioned by run_date with Idempotency."""
    logger = get_run_logger()
    df_clean['run_date'] = run_date
    table = pa.Table.from_pandas(df_clean)
    
    dataset_path = os.path.join(clean_dir, db_name, table_name.replace(" ", "_"))
    
    # ---------------------------------------------------------
    # 🛑 Idempotency Fix: เคลียร์พาร์ทิชันเก่าของวันนี้ทิ้งก่อนเขียนใหม่
    # ---------------------------------------------------------
    partition_path = os.path.join(dataset_path, f"run_date={run_date}")
    if os.path.exists(partition_path):
        logger.info(f"♻️ Found existing partition for {run_date}. Cleaning up to prevent data duplication...")
        shutil.rmtree(partition_path)

    # เขียนข้อมูลลง Parquet
    pq.write_to_dataset(
        table, 
        root_path=dataset_path, 
        partition_cols=['run_date'], 
        existing_data_behavior='overwrite_or_ignore'
    )
    logger.info(f"✅ Saved clean data to: {dataset_path}")