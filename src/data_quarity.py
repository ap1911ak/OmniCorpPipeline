import numpy as np
import pandas as pd



def estimate_dataframe(df):
    total_rows = len(df)

    if total_rows == 0:
        print("Dataframe is empty.")
        return None

    # --- ส่วนที่ 1: คำนวณหาภาพรวมแถวซ้ำ (Duplicate Rows) ---
    # ค้นหาแถวที่ซ้ำกันทั้งหมด (ไม่นับการปรากฏตัวครั้งแรก)
    duplicate_rows_count = df.duplicated().sum()
    duplicate_rows_pct = (duplicate_rows_count / total_rows) * 100

    # --- ส่วนที่ 2: คำนวณรายคอลัมน์ (Missing, Outliers, Column Duplicates) ---
    missing_count = df.isnull().sum()
    missing_pct = (missing_count / total_rows) * 100

    outlier_counts = []
    outlier_pcts = []
    col_duplicate_pcts = []

    for col in df.columns:
        # 1. คำนวณ % ข้อมูลซ้ำในคอลัมน์ (Column-level Duplicates)
        # สูตร: (จำนวนแถวทั้งหมด - จำนวนค่าที่ไม่ซ้ำกัน) / จำนวนแถวทั้งหมด
        # รวมค่า NaN เป็นค่าซ้ำด้วย หากต้องการละเว้นให้ใช้ df[col].dropna().duplicated().sum()
        col_dup_count = df[col].duplicated().sum()
        col_dup_pct = (col_dup_count / total_rows) * 100
        col_duplicate_pcts.append(col_dup_pct)

        # 2. คำนวณ Outliers (เฉพาะ Numeric)
        if pd.api.types.is_numeric_dtype(df[col]):
            col_data = df[col].dropna()
            if len(col_data) > 0:
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1

                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                outliers = col_data[
                    (col_data < lower_bound) | (col_data > upper_bound)
                ]
                count = len(outliers)
                pct = (count / total_rows) * 100
            else:
                count, pct = 0, 0.0
        else:
            count, pct = np.nan, np.nan

        outlier_counts.append(count)
        outlier_pcts.append(pct)

    # --- ส่วนที่ 3: ประกอบตารางสรุปรายคอลัมน์ ---
    summary_df = pd.DataFrame(
        {
            "Data Type": df.dtypes,
            "Missing Count": missing_count,
            "Missing (%)": missing_pct,
            "Col Duplicate (%)": col_duplicate_pcts,  # คอลัมน์ที่เพิ่มเข้ามาใหม่
            "Outlier Count": outlier_counts,
            "Outlier (%)": outlier_pcts,
        }
    )

    # จัดรูปแบบตัวเลขเพื่อความสวยงามและการอ่านที่ง่ายขึ้น
    formatted_df = summary_df.copy()
    formatted_df["Missing (%)"] = formatted_df["Missing (%)"].apply(
        lambda x: f"{x:.2f}%"
    )
    formatted_df["Col Duplicate (%)"] = formatted_df["Col Duplicate (%)"].apply(
        lambda x: f"{x:.2f}%"
    )
    formatted_df["Outlier Count"] = formatted_df["Outlier Count"].apply(
        lambda x: f"{int(x)}" if pd.notnull(x) else "N/A"
    )
    formatted_df["Outlier (%)"] = formatted_df["Outlier (%)"].apply(
        lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A"
    )

    # พิมพ์สรุปความซ้ำซ้อนระดับแถว (Row-level Duplicates) ออกทางหน้าจอ
    print("=" * 60)
    print("DATAFRAME ROW-LEVEL DUPLICATE SUMMARY")
    print("=" * 60)
    print(f"Total Rows: {total_rows}")
    print(f"Duplicate Rows Count: {duplicate_rows_count}")
    print(f"Duplicate Rows Percentage: {duplicate_rows_pct:.2f}%")
    print("=" * 60 + "\n")

    return formatted_df