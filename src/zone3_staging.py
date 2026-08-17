import pandas as pd
import numpy as np
from pathlib import Path
from prefect import task, get_run_logger

def _load_latest_parquet_table(clean_dir: Path, db_name: str, table_name: str) -> pd.DataFrame:
    """
    Helper to read parquet files. 
    Modified to read ONLY the latest partition (run_date) to prevent historical duplication.
    """
    target_dir = clean_dir / db_name / table_name
    
    if not target_dir.exists():
        raise FileNotFoundError(f"Directory not found for {db_name}.{table_name} at {target_dir}")
        
    partitions = [d for d in target_dir.iterdir() if d.is_dir() and "run_date=" in d.name]
    
    if partitions:
        latest_partition = max(partitions, key=lambda x: x.name)
        files = list(latest_partition.rglob("*.parquet"))
        get_run_logger().info(f"Reading latest partition: {latest_partition.name} for {table_name}")
    else:
        files = list(target_dir.rglob("*.parquet"))
        
    if not files:
        raise FileNotFoundError(f"No parquet files found in {target_dir}")
        
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


@task(name="Zone 3: Build Dim Source System")
def build_dim_source_system() -> pd.DataFrame:
    get_run_logger().info("Building dim_source_system...")
    return pd.DataFrame([
        {"source_system_id": np.int32(1), "source_system_name": "Chinook", "description": "Digital Media Store"},
        {"source_system_id": np.int32(2), "source_system_name": "Northwind", "description": "Food and Beverage"}
    ])


@task(name="Zone 3: Build Dim Date")
def build_dim_date(start_date="2000-01-01", end_date="2030-12-31") -> pd.DataFrame:
    get_run_logger().info("Building dim_date...")
    df = pd.DataFrame({"full_date": pd.date_range(start=start_date, end=end_date, freq="D")})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype("int32")
    df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
    df["day_of_month"] = pd.DatetimeIndex(df["full_date"]).day.astype("int32")
    df["day_of_week"] = pd.DatetimeIndex(df["full_date"]).day_name()
    df["month"] = pd.DatetimeIndex(df["full_date"]).month.astype("int32")
    df["month_name"] = pd.DatetimeIndex(df["full_date"]).month_name()
    df["quarter"] = pd.DatetimeIndex(df["full_date"]).quarter.astype("int32")
    df["year"] = pd.DatetimeIndex(df["full_date"]).year.astype("int32")
    return df


@task(name="Zone 3: Build Dim Customers")
def build_dim_customers(clean_dir: Path) -> pd.DataFrame:
    get_run_logger().info("Building dim_customers...")
    
    ch_cust = _load_latest_parquet_table(clean_dir, "chinook", "Customer")
    ch_df = pd.DataFrame({
        "customer_id": "CH_" + ch_cust["CustomerId"].astype(str),
        "company_name": ch_cust.get("Company", pd.Series(dtype=str)).fillna("N/A"),
        "contact_first_name": ch_cust.get("FirstName", pd.Series(dtype=str)),
        "contact_last_name": ch_cust.get("LastName", pd.Series(dtype=str)),
        "contact_title": "N/A",
        "address": ch_cust.get("Address", pd.Series(dtype=str)),
        "city": ch_cust.get("City", pd.Series(dtype=str)),
        "state_region": ch_cust.get("State", pd.Series(dtype=str)),
        "postal_code": ch_cust.get("PostalCode", pd.Series(dtype=str)),
        "country": ch_cust.get("Country", pd.Series(dtype=str)),
        "phone": ch_cust.get("Phone", pd.Series(dtype=str)),
        "email": ch_cust.get("Email", pd.Series(dtype=str)),
        "support_rep_id": ch_cust.get("SupportRepId").apply(lambda x: f"CH_{int(x)}" if pd.notna(x) else "N/A"),
        "source_system": "Chinook"
    })

    nw_cust = _load_latest_parquet_table(clean_dir, "northwind", "Customers")
    names = nw_cust["ContactName"].fillna("UNKNOWN").str.split(" ", n=1, expand=True)
    nw_df = pd.DataFrame({
        "customer_id": "NW_" + nw_cust["CustomerID"].astype(str),
        "company_name": nw_cust.get("CompanyName", pd.Series(dtype=str)),
        "contact_first_name": names[0],
        "contact_last_name": names[1].fillna("UNKNOWN") if names.shape[1] > 1 else "UNKNOWN",
        "contact_title": nw_cust.get("ContactTitle", pd.Series(dtype=str)),
        "address": nw_cust.get("Address", pd.Series(dtype=str)),
        "city": nw_cust.get("City", pd.Series(dtype=str)),
        "state_region": nw_cust.get("Region", pd.Series(dtype=str)),
        "postal_code": nw_cust.get("PostalCode", pd.Series(dtype=str)),
        "country": nw_cust.get("Country", pd.Series(dtype=str)),
        "phone": nw_cust.get("Phone", pd.Series(dtype=str)),
        "email": "N/A",
        "support_rep_id": "N/A",
        "source_system": "Northwind"
    })
    
    return pd.concat([ch_df, nw_df], ignore_index=True).fillna("N/A")


@task(name="Zone 3: Build Dim Employees")
def build_dim_employees(clean_dir: Path) -> pd.DataFrame:
    get_run_logger().info("Building dim_employees...")
    
    ch_emp = _load_latest_parquet_table(clean_dir, "chinook", "Employee")
    ch_df = pd.DataFrame({
        "employee_id": "CH_" + ch_emp["EmployeeId"].astype(str),
        "first_name": ch_emp.get("FirstName", pd.Series(dtype=str)),
        "last_name": ch_emp.get("LastName", pd.Series(dtype=str)),
        "title": ch_emp.get("Title", pd.Series(dtype=str)),
        "title_of_courtesy": "N/A",
        "birth_date": pd.to_datetime(ch_emp.get("BirthDate")).dt.date,
        "hire_date": pd.to_datetime(ch_emp.get("HireDate")).dt.date,
        "address": ch_emp.get("Address", pd.Series(dtype=str)),
        "city": ch_emp.get("City", pd.Series(dtype=str)),
        "state_region": ch_emp.get("State", pd.Series(dtype=str)),
        "postal_code": ch_emp.get("PostalCode", pd.Series(dtype=str)),
        "country": ch_emp.get("Country", pd.Series(dtype=str)),
        "phone": ch_emp.get("Phone", pd.Series(dtype=str)),
        "extension": "N/A",
        "email": ch_emp.get("Email", pd.Series(dtype=str)),
        "reports_to": ch_emp.get("ReportsTo").apply(lambda x: f"CH_{int(x)}" if pd.notna(x) else "N/A"),
        "source_system": "Chinook"
    })

    nw_emp = _load_latest_parquet_table(clean_dir, "northwind", "Employees")
    nw_df = pd.DataFrame({
        "employee_id": "NW_" + nw_emp["EmployeeID"].astype(str),
        "first_name": nw_emp.get("FirstName", pd.Series(dtype=str)),
        "last_name": nw_emp.get("LastName", pd.Series(dtype=str)),
        "title": nw_emp.get("Title", pd.Series(dtype=str)),
        "title_of_courtesy": nw_emp.get("TitleOfCourtesy", pd.Series(dtype=str)),
        "birth_date": pd.to_datetime(nw_emp.get("BirthDate")).dt.date,
        "hire_date": pd.to_datetime(nw_emp.get("HireDate")).dt.date,
        "address": nw_emp.get("Address", pd.Series(dtype=str)),
        "city": nw_emp.get("City", pd.Series(dtype=str)),
        "state_region": nw_emp.get("Region", pd.Series(dtype=str)),
        "postal_code": nw_emp.get("PostalCode", pd.Series(dtype=str)),
        "country": nw_emp.get("Country", pd.Series(dtype=str)),
        "phone": nw_emp.get("HomePhone", pd.Series(dtype=str)),
        "extension": nw_emp.get("Extension", pd.Series(dtype=str)),
        "email": "N/A",
        "reports_to": nw_emp.get("ReportsTo").apply(lambda x: f"NW_{int(x)}" if pd.notna(x) else "N/A"),
        "source_system": "Northwind"
    })

    return pd.concat([ch_df, nw_df], ignore_index=True).fillna("N/A")


@task(name="Zone 3: Build Dim Products")
def build_dim_products(clean_dir: Path) -> pd.DataFrame:
    get_run_logger().info("Building dim_products...")
    
    ch_track = _load_latest_parquet_table(clean_dir, "chinook", "Track")
    ch_genre = _load_latest_parquet_table(clean_dir, "chinook", "Genre")
    ch_merged = ch_track.merge(ch_genre, on="GenreId", how="left", suffixes=("", "_genre"))
    
    ch_df = pd.DataFrame({
        "product_id": "CH_" + ch_merged["TrackId"].astype(str),
        "product_name": ch_merged["Name"],
        "product_type": "Digital Music Track",
        "category_name": "Music",
        "genre_name": ch_merged.get("Name_genre", pd.Series(dtype=str)),
        "composer_supplier": ch_merged.get("Composer", pd.Series(dtype=str)),
        "unit_price": ch_merged.get("UnitPrice", 0.99).astype(float)
    })

    nw_prod = _load_latest_parquet_table(clean_dir, "northwind", "Products")
    nw_cat = _load_latest_parquet_table(clean_dir, "northwind", "Categories")
    nw_sup = _load_latest_parquet_table(clean_dir, "northwind", "Suppliers")
    
    nw_merged = nw_prod.merge(nw_cat, on="CategoryID", how="left")
    nw_merged = nw_merged.merge(nw_sup, on="SupplierID", how="left", suffixes=("", "_sup"))

    nw_df = pd.DataFrame({
        "product_id": "NW_" + nw_merged["ProductID"].astype(str),
        "product_name": nw_merged["ProductName"],
        "product_type": "Food & Beverage Item",
        "category_name": nw_merged.get("CategoryName", pd.Series(dtype=str)),
        "genre_name": "N/A",
        "composer_supplier": nw_merged.get("CompanyName", pd.Series(dtype=str)),
        "unit_price": nw_merged.get("UnitPrice", 0.0).astype(float)
    })

    return pd.concat([ch_df, nw_df], ignore_index=True).fillna("N/A")


@task(name="Zone 3: Build Fact Orders")
def build_fact_orders(clean_dir: Path) -> pd.DataFrame:
    get_run_logger().info("Building fact_orders...")
    facts = []

    ch_inv = _load_latest_parquet_table(clean_dir, "chinook", "Invoice")
    ch_line = _load_latest_parquet_table(clean_dir, "chinook", "InvoiceLine")
    ch_cust = _load_latest_parquet_table(clean_dir, "chinook", "Customer")
    
    ch_inv_cust = ch_inv.merge(ch_cust[["CustomerId", "SupportRepId"]], on="CustomerId", how="left")
    ch_merged = ch_line.merge(ch_inv_cust[["InvoiceId", "CustomerId", "SupportRepId", "InvoiceDate"]], on="InvoiceId", how="inner")
    
    ch_fact = pd.DataFrame({
        "date_key": pd.to_datetime(ch_merged["InvoiceDate"]).dt.strftime("%Y%m%d").astype("int32"),
        "customer_id": "CH_" + ch_merged["CustomerId"].astype(str),
        "employee_id": ch_merged["SupportRepId"].apply(lambda x: f"CH_{int(x)}" if pd.notna(x) else "UNKNOWN"),
        "product_id": "CH_" + ch_merged["TrackId"].astype(str),
        "source_system_id": np.int32(1),
        "source_order_id": "CH_" + ch_merged["InvoiceId"].astype(str),
        "unit_price": ch_merged["UnitPrice"].astype(float),
        "quantity": ch_merged["Quantity"].astype("int32"),
        "discount": 0.0
    })
    ch_fact["sales_amount"] = ch_fact["unit_price"] * ch_fact["quantity"]
    facts.append(ch_fact)

    nw_ord = _load_latest_parquet_table(clean_dir, "northwind", "Orders")
    nw_line = _load_latest_parquet_table(clean_dir, "northwind", "Order_Details")
    nw_merged = nw_line.merge(nw_ord[["OrderID", "CustomerID", "EmployeeID", "OrderDate"]], on="OrderID", how="inner")
    
    nw_fact = pd.DataFrame({
        "date_key": pd.to_datetime(nw_merged["OrderDate"]).dt.strftime("%Y%m%d").astype("int32"),
        "customer_id": "NW_" + nw_merged["CustomerID"].astype(str),
        "employee_id": "NW_" + nw_merged["EmployeeID"].astype(str),
        "product_id": "NW_" + nw_merged["ProductID"].astype(str),
        "source_system_id": np.int32(2),
        "source_order_id": "NW_" + nw_merged["OrderID"].astype(str),
        "unit_price": nw_merged["UnitPrice"].astype(float),
        "quantity": nw_merged["Quantity"].astype("int32"),
        "discount": nw_merged.get("Discount", 0.0).astype(float)
    })
    nw_fact["sales_amount"] = (nw_fact["unit_price"] * nw_fact["quantity"]) * (1.0 - nw_fact["discount"])
    facts.append(nw_fact)

    fact_orders = pd.concat(facts, ignore_index=True)
    
    fact_orders["sales_key"] = pd.util.hash_pandas_object(
        fact_orders[['source_system_id', 'source_order_id', 'product_id']], 
        index=False
    ).astype(np.uint64)
    
    return fact_orders