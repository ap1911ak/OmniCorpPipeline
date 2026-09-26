import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import json
import datetime
import altair as alt
import duckdb
try:
    from pandarallel import pandarallel
    pandarallel.initialize(progress_bar=False, verbose=0)
    HAS_PARALLEL = False
except ImportError:
    HAS_PARALLEL = False

named = lambda fn, n: (setattr(fn, "__name__", n) or fn)
def case_boolean(x):
    mapping = {"true": True, "false": False, "1": True, "0": False, "y": True, "n": False}
    key = str(x).strip().lower()
    try: return mapping[key]
    except KeyError: raise ValueError(f"Invalid boolean: {x}")

TYPE_SPECS = {
    "string": {
        "default": None,
        "dtype": pa.string(),
        "func": named(lambda x: x if pd.isna(x) else str(x), "case_string"),
        "coerce": lambda s: s.astype("string"),
    },
    "integer": {
        "default": None,
        "dtype": pa.int64(),
        "func": named(lambda x: int(pd.to_numeric(x, errors="raise")), "case_integer"),
        "coerce": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    },
    "float": {
        "default": np.nan,
        "dtype": pa.float64(),
        "func": named(lambda x: float(pd.to_numeric(x, errors="raise")), "case_float"),
        "coerce": lambda s: pd.to_numeric(s, errors="coerce"),
    },
    "datetime": {
        "default": pd.NaT,
        "dtype": pa.timestamp("ns"),
        "func": named(lambda x: pd.to_datetime(x, errors="raise"), "case_datetime"),
        "coerce": lambda s: pd.to_datetime(s, errors="coerce"),
    },
    "boolean": {
        "default": None,
        "dtype": pa.bool_(),
        "func": case_boolean,
        "coerce": lambda s: s.astype("boolean"),
    },
}


class Monad:
    __slots__ = ['value', 'input_row', 'output_column', 'status', 'final_value', 'logs', 'stopped', 'step', 'last_role', 'transformer_succeeded']
    def __init__(self, value, row=None, col=None):
        self.value = value
        self.input_row = row
        self.output_column = col

        self.status = 'pending'
        self.final_value = None
        self.logs = []
        self.stopped = False       # only for hard-stop (validator fail)
        self.step = 0

        self.last_role = None
        self.transformer_succeeded = False  # at least one T passed

    def _log(self, func, status, details=None, value=None):
        role = getattr(func, '_role', 'validator')
        name = getattr(func, '__name__', 'unknown')
        self.logs.append({
            'row': self.input_row,
            'col': self.output_column,
            'step': self.step,
            'role_type': role,
            'role': name,
            'status': status,
            'value': str(self.value if value is None else value),
            'details': details,
        })

    def __or__(self, func):
        if self.stopped:
            self.step += 1
            self._log(func, 'skipped')
            return self

        self.step += 1
        role = getattr(func, '_role', 'validator')
        self.last_role = role

        if role == 'transformer' and self.transformer_succeeded:
            self._log(func, 'skipped')
            return self

        input_value = self.value
        try:
            res = func(input_value)

            if role == 'validator':
                self.value = res
                if self.status not in ('success', 'dirty'):
                    self.status = 'valid'
                self._log(func, 'passed', value=input_value)

            elif role == 'transformer':
                self.value = res
                self.status = 'success'
                self.final_value = res
                self.transformer_succeeded = True
                self._log(func, 'passed', value=input_value)

        except Exception as e:
            if role == 'validator':
                self.status = 'dirty'
                self.final_value = None
                self.stopped = True
                self._log(func, 'failed', str(e), value=input_value)
            elif role == 'transformer':
                self._log(func, 'failed', str(e), value=input_value)

        return self

    def apply(self, pipeline):
        transformer_seen = False

        for func in pipeline:
            if getattr(func, '_role', 'validator') == 'transformer':
                transformer_seen = True
            self | func

        if self.status == 'dirty':
            self.final_value = None
        else:
            if self.transformer_succeeded:
                self.status = 'success'
                self.final_value = self.value
            else:
                # UPDATED LOGIC: If transformers were attempted (transformer_seen) but none succeeded,
                # we must treat this as a failure, even if the pipeline ended on a validator (like type casting).
                # This prevents raw multi-source dicts from being cast to strings like "{'a':1}" when transformation fails.
                if transformer_seen:
                    self.status = 'dirty'
                    self.final_value = None
                    self.logs.append({
                        'row': self.input_row,
                        'col': self.output_column,
                        'step': self.step + 1,
                        'role_type': 'system',
                        'role': 'chain_exhausted',
                        'status': 'failed',
                        'details': 'All transformers failed (and strictly required)'
                    })
                else:
                    # No transformers involved (Validation only pipeline)
                    self.status = 'success'
                    self.final_value = self.value

        return self

# Version 3 Support many source input columns
class SquishyEngine:
    def __init__(self, config_list, source_df):
        self.config = config_list
        self.df = source_df.reset_index(drop=True)
        self.logs = []
        self.final_df = None

    def run(self):
        print(f"Processing {len(self.df)} rows...")
        final_df = pd.DataFrame(index=self.df.index)
        all_logs = []

        for col_def in self.config:
            target = col_def["target"]
            source = col_def.get("source", target)

            # --- UPDATED: Check for column existence (Handle List vs String) ---
            missing_cols = []
            if isinstance(source, list):
                missing_cols = [c for c in source if c not in self.df.columns]
            elif source not in self.df.columns:
                missing_cols = [source]
            
            if missing_cols:
                print(f"Warning: Missing source column(s) {missing_cols} for target '{target}'. Skipping.")
                continue
            # -------------------------------------------------------------------

            spec = TYPE_SPECS[col_def.get("type", "string")]
            pipeline = list(col_def["pipeline"]) + [spec["func"]]

            def process(row):
                # --- UPDATED: Extract Data (Handle List vs String) ---
                if isinstance(source, list):
                    # Multi-source: Pass a Dictionary of values
                    initial_val = {col: row[col] for col in source}
                else:
                    # Single-source: Pass the scalar value
                    initial_val = row[source]
                # -----------------------------------------------------

                m = Monad(initial_val, row.name, target).apply(pipeline)
                
                # Determine return value
                value = m.final_value if m.status == "success" else spec["default"]
                return value, m.logs

            # Execution
            if HAS_PARALLEL:
                results = self.df.parallel_apply(process, axis=1)
            else:
                results = self.df.apply(process, axis=1)

            # Unpack results
            values = results.map(lambda x: x[0])
            for log_list in results.map(lambda x: x[1]):
                all_logs.extend(log_list)

            # Finalize column
            values = values.replace("<NA>", pd.NA)
            final_df[target] = spec["coerce"](values)

        self.final_df = final_df
        self.logs = pd.DataFrame(all_logs)
        return final_df

# --- Validator & Transformer ---
def validator(f): f._role = 'validator'; return f
def transformer(f): f._role = 'transformer'; return f

# --- RICH REPORTING FUNCTIONS (UPDATED) ---
def get_top_values(series, n=5):
    """
    Helper: Returns top N frequent values as a string "Value (Count)"
    """
    if series.empty: return "None"
    # Convert to string to handle mixed types
    counts = series.astype(str).value_counts().head(n)
    return ", ".join([f"'{k}' ({v})" for k, v in counts.items()])

def generate_rich_dq_report(engine, df_raw):
    """
    Generates the JSON structure for the report.
    """
    print("Generating rich metadata with top 5 value analysis...")
    chart_data = []
    
    unique_targets = set(c['target'] for c in engine.config)
    
    for target_col in unique_targets:
        config_item = next(c for c in engine.config if c['target'] == target_col)
        source_col = config_item.get('source', target_col)
        
        if engine.logs.empty:
            # Assume all passed if no logs
            passed_rows = df_raw.index
            invalid_rows = []
            missing_rows = []
        else:
            col_logs = engine.logs[engine.logs['col'] == target_col]
            
            # FAILED (Invalid)
            invalid_mask = (col_logs['status'] == 'failed') & (col_logs['details'] != 'Missing')
            invalid_rows = col_logs[invalid_mask]['row'].unique()
            
            # MISSING
            missing_mask = col_logs['details'] == 'Missing'
            missing_rows = col_logs[missing_mask]['row'].unique()
            
            # PASSED
            passed_mask = (col_logs['step'] == col_logs.groupby('row')['step'].transform('max')) & (col_logs['status'] == 'passed')
            passed_rows = col_logs[passed_mask]['row'].unique()


        # EXTRACT TOP 5 VALUES
        raw_series = df_raw[source_col]
        
        vals_invalid = raw_series.loc[raw_series.index.isin(invalid_rows)]
        top5_invalid = get_top_values(vals_invalid)
        
        vals_missing = raw_series.loc[raw_series.index.isin(missing_rows)]
        top5_missing = get_top_values(vals_missing) if not vals_missing.empty else "None"
        
        vals_passed = raw_series.loc[raw_series.index.isin(passed_rows)]
        top5_passed = get_top_values(vals_passed)
        
        chart_data.append({
            "Column": target_col, "Status": "Passed", "Count": len(passed_rows), "Top_Issues": top5_passed
        })
        chart_data.append({
            "Column": target_col, "Status": "Invalid", "Count": len(invalid_rows), "Top_Issues": top5_invalid
        })
        chart_data.append({
            "Column": target_col, "Status": "Missing", "Count": len(missing_rows), "Top_Issues": top5_missing
        })

    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "total_rows": len(df_raw),
        "chart_data": chart_data
    }

def serialize_config(config):
    return [
        {
            "target": r["target"],
            "source": r["source"],
            "pipeline": [
                f"{fn.__name__}"
                for fn in r["pipeline"]
            ],
        }
        for r in config
    ]

def save_parquet_with_metadata(df, report, config, con, path):
    dq_report_str = json.dumps(report).replace("'", "''")        # escape single quotes
    dq_config_str = json.dumps(serialize_config(config)).replace("'", "''")

    con.register("_temp_df", df)
    con.execute(f"""
        COPY _temp_df TO '{path}' (
            FORMAT parquet,
            KV_METADATA {{
                dq_report: '{dq_report_str}',
                dq_config: '{dq_config_str}'
            }}
        )
    """)
    con.unregister("_temp_df")
    print(f"✅ Saved {path} with embedded metadata")

def read_parquet_with_metadata(path, con):
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").df()
    
    meta = con.execute(f"""
        SELECT value 
        FROM parquet_kv_metadata('{path}')
    """).fetchall()
    
    print("Metadata:", meta)
    return df

def load_sq_config(path, con):
    rows = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    meta = {row[0]: row[1] for row in rows}  # {bytes_key: bytes_value}
    return (
        json.loads(meta[b"dq_config"].decode("utf-8"))
        if b"dq_config" in meta
        else None
    )
###########################################################################################################################################################################################
    

def _build_altair_chart(df_viz, path, sort_order=None):
    """Helper function สำหรับประกอบร่าง Altair Chart แนวนอน"""
    import altair as alt # เผื่อกรณีไฟล์นี้ยังไม่ได้ import
    
    chart = (
        alt.Chart(df_viz)
        .mark_bar()
        .encode(
            x=alt.X("Count:Q", stack="normalize", axis=alt.Axis(format="%", title="Percentage")),
            y=alt.Y("Column:N", sort=sort_order if sort_order else "ascending"),
            color=alt.Color("Status:N", scale=alt.Scale(
                domain=["Passed", "Missing", "Invalid"],
                range=["#2ca02c", "#ff7f0e", "#d62728"]
            )),
            order=alt.Order("Status", sort="descending"),
            tooltip=[
                alt.Tooltip("Column:N", title="Field"),
                alt.Tooltip("Status:N", title="Status"),
                alt.Tooltip("Count:Q", title="Row Count", format=","),
                alt.Tooltip("Top_Issues:N", title="Top 5 Values"),
            ],
        )
        .properties(
            title=f"Data Quality Profiling: {path.split('/')[-1]}",
            width=700,
            # 💡 ปรับความสูงกราฟอัตโนมัติตามจำนวนคอลัมน์
            height=max(300, len(df_viz['Column'].unique()) * 25), 
        )
    )
    return chart

def visualize_from_parquet(path, con=None):
    """
    สร้างกราฟ Data Profiling จากไฟล์ Parquet
    รองรับทั้งไฟล์ที่มี DQ Metadata ฝังอยู่ และไฟล์ Parquet ปกติ (Auto-Profiling)
    """
    import pandas as pd
    import json
    
    if con is None:
        import duckdb
        con = duckdb

    # ---------------------------------------------------------
    # สเต็ปที่ 1: ลองอ่าน Metadata ที่ฝังมากับไฟล์
    # ---------------------------------------------------------
    try:
        rows = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
        meta = {row[0]: row[1] for row in rows}
        
        if b"dq_report" in meta:
            report = json.loads(meta[b"dq_report"].decode("utf-8"))
            df_viz = pd.DataFrame(report["chart_data"])
            
            sort_order = None
            if b"dq_config" in meta:
                try:
                    config = json.loads(meta[b"dq_config"].decode("utf-8"))
                    sort_order = [item["target"] for item in config]
                except:
                    pass

            return _build_altair_chart(df_viz, path, sort_order)
    except Exception:
        pass # ถ้าอ่านไม่ได้ ให้ไหลไปทำงานในสเต็ปที่ 2

    # ---------------------------------------------------------
    # สเต็ปที่ 2: FALLBACK MODE (ใช้ DuckDB สแกนข้อมูลจริงเพื่อทำ Profiling)
    # ---------------------------------------------------------
    print(f"💡 ไม่พบ DQ Metadata... ระบบกำลังทำ Auto-Profiling โดยสแกนข้อมูลจริงจากไฟล์แทน")
    try:
        cols_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").df()
        cols = cols_df['column_name'].tolist()
        chart_data = []
        
        for col in cols:
            # 💡 เพิ่ม '<NA>' เข้าไปในเงื่อนไขการดักจับค่าว่าง
            query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN "{col}" IS NULL OR CAST("{col}" AS VARCHAR) IN ('', 'nan', 'None', '<NA>') THEN 1 ELSE 0 END) as missing
            FROM read_parquet('{path}')
            """
            res = con.execute(query).fetchone()
            total = res[0]
            missing = int(res[1]) if res[1] else 0
            passed = total - missing
            
            # 💡 เพิ่ม '<NA>' เข้าไปในการกรองข้อมูล Top 5 ด้วย
            top5_query = f"""
            SELECT CAST("{col}" AS VARCHAR) as val, COUNT(*) as cnt
            FROM read_parquet('{path}')
            WHERE "{col}" IS NOT NULL AND CAST("{col}" AS VARCHAR) NOT IN ('', 'nan', 'None', '<NA>')
            GROUP BY val ORDER BY cnt DESC LIMIT 5
            """
            try:
                top5_rows = con.execute(top5_query).fetchall()
                top5_str = ", ".join([f"'{r[0]}' ({r[1]})" for r in top5_rows]) if top5_rows else "None"
            except:
                top5_str = "Error extracting values"
                
            if passed > 0:
                chart_data.append({"Column": col, "Status": "Passed", "Count": passed, "Top_Issues": top5_str})
            if missing > 0:
                chart_data.append({"Column": col, "Status": "Missing", "Count": missing, "Top_Issues": "N/A (Null/Empty)"})
        
        df_viz = pd.DataFrame(chart_data)
        
        # คืนค่าตัวกราฟออกไป โดยเรียงตามลำดับคอลัมน์จริง
        return _build_altair_chart(df_viz, path, sort_order=cols)
        
    except Exception as e:
        print(f"❌ Auto-Profiling ล้มเหลว: {e}")
        return None

class DuckDBEngine:
    def __init__(self, config, con, source_view):
        self.config = config
        self.con = con
        self.source_view = source_view
        
    def generate_sql(self):
        """ประกอบร่าง SQL Query จาก Config"""
        select_exprs = []
        for col in self.config:
            target = col['target']
            transform_sql = col.get('transform', col['source'])
            select_exprs.append(f"{transform_sql} AS {target}")
        
        # เพิ่ม DISTINCT เพื่อทำ Deduplicate อัตโนมัติ (ถ้ารหัส id ซ้ำกัน)
        return f"SELECT DISTINCT * FROM (SELECT {', '.join(select_exprs)} FROM {self.source_view})"

    def run(self, output_view_name="staging_transformed"):
        """ประมวลผลการ Transform และสร้างเป็น View ใน DuckDB"""
        sql = self.generate_sql()
        self.con.execute(f"CREATE OR REPLACE VIEW {output_view_name} AS {sql}")
        return self.con.execute(f"SELECT * FROM {output_view_name}").df()
        
    def profile_data(self, view_name="staging_transformed"):
        """สร้าง Data Quality Report (Aggregate)"""
        chart_data = []
        for col in self.config:
            target = col['target']
            validate_sql = col.get('validate', f"{target} IS NOT NULL")
            
            # นับจำนวนข้อมูลที่ ผ่าน/ไม่ผ่าน
            stats_query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN NOT ({validate_sql}) OR {target} IS NULL THEN 1 ELSE 0 END) as invalid
            FROM {view_name}
            """
            res = self.con.execute(stats_query).fetchone()
            total, invalid = res[0], int(res[1] or 0)
            passed = total - invalid
            
            # ดึง Top 5 ค่าที่พบบ่อย (สำหรับข้อมูลที่ Valid)
            top5_query = f"""
            SELECT CAST({target} AS VARCHAR) as val, COUNT(*) as cnt
            FROM {view_name}
            WHERE {target} IS NOT NULL AND CAST({target} AS VARCHAR) != ''
            GROUP BY val ORDER BY cnt DESC LIMIT 5
            """
            try:
                top5_rows = self.con.execute(top5_query).fetchall()
                top5_str = ", ".join([f"'{r[0]}' ({r[1]})" for r in top5_rows]) if top5_rows else "None"
            except:
                top5_str = "N/A"
            
            if passed > 0:
                chart_data.append({"Column": target, "Status": "Passed", "Count": passed, "Top_Issues": top5_str})
            if invalid > 0:
                chart_data.append({"Column": target, "Status": "Missing/Invalid", "Count": invalid, "Top_Issues": "N/A"})
                
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "chart_data": chart_data
        }

def save_parquet_with_metadata(con, view_name, report, config, path):
    """ยิงข้อมูลจาก DuckDB ลงไฟล์ Parquet ตรงๆ พร้อมฝัง Metadata"""
    dq_report_str = json.dumps(report).replace("'", "''")
    dq_config_str = json.dumps([{"target": c["target"]} for c in config]).replace("'", "''")
    
    con.execute(f"""
        COPY (SELECT * FROM {view_name}) TO '{path}' (
            FORMAT parquet,
            KV_METADATA {{
                dq_report: '{dq_report_str}',
                dq_config: '{dq_config_str}'
            }}
        )
    """)
    print(f"✅ บันทึก Good Data และ Metadata ลงที่: {path}")

# Version 2
# class SquishyEngine:
#     def __init__(self, config_list, source_df):
#         self.config = config_list
#         self.df = source_df.reset_index(drop=True)
#         self.logs = []
#         self.final_df = None

#     def run(self):
#         print(f"Processing {len(self.df)} rows...")
#         final_df = pd.DataFrame(index=self.df.index)
#         all_logs = []

#         for col_def in self.config:
#             target = col_def["target"]
#             source = col_def.get("source", target)

#             if source not in self.df.columns:
#                 continue

#             spec = TYPE_SPECS[col_def.get("type", "string")]

#             # 🔒 NEVER mutate the original pipeline
#             pipeline = list(col_def["pipeline"]) + [spec["func"]]

#             def process(row):
#                 m = Monad(row[source], row.name, target).apply(pipeline)
#                 value = m.final_value if m.status == "success" else spec["default"]
#                 return value, m.logs

#             results = (
#                 self.df.parallel_apply(process, axis=1)
#                 if HAS_PARALLEL
#                 else self.df.apply(process, axis=1)
#             )

#             # extract values + logs
#             values = results.map(lambda x: x[0])
#             for log_list in results.map(lambda x: x[1]):
#                 all_logs.extend(log_list)

#             # 🧼 sanitize fake missing BEFORE coercion
#             values = values.replace("<NA>", pd.NA)

#             # 🔧 enforce dtype
#             final_df[target] = spec["coerce"](values)

#         self.final_df = final_df
#         self.logs = pd.DataFrame(all_logs)
#         return final_df


# Version 1
# class SquishyEngine:
#     def __init__(self, config_list, source_df):
#         self.config = config_list; self.df = source_df.reset_index(drop=True); self.logs = []; self.final_df = None

#     def run(self):
#         print(f"Processing {len(self.df)} rows...")
#         final_df = pd.DataFrame(index=self.df.index)
#         all_logs = []

#         for col_def in self.config:
#             target = col_def['target']; source = col_def.get('source', target)
#             pipeline = col_def['pipeline'] 
#             spec = TYPE_SPECS.get(col_def.get('type', 'string'))
#             pipeline.append(spec['func'])
#             if source not in self.df.columns: 
#                 continue

#             def process(row):
#                 m = Monad(row[source], row.name, target).apply(pipeline)
#                 return (m.final_value if m.status == 'success' else spec['default'], m.logs)

#             if HAS_PARALLEL: 
#                 results = self.df.parallel_apply(process, axis=1)
#             else: 
#                 results = self.df.apply(process, axis=1)

#             final_df[target] = results.apply(lambda x: x[0])
#             for log_list in results.apply(lambda x: x[1]): all_logs.extend(log_list)
#             final_df[target] = spec['coerce'](final_df[target])

#         self.final_df = final_df
#         self.logs = pd.DataFrame(all_logs)
#         return final_df

### Old Version (Using s3fs)
# def save_parquet_with_metadata(df, report, config, fs, path):
#     table = pa.Table.from_pandas(df)
#     meta_dict = { 
#         b"dq_report": json.dumps(report).encode("utf-8"),
#         b"sq_config": json.dumps(serialize_config(config)).encode("utf-8"),
#     }
#     existing = table.schema.metadata or {}
#     merged_meta = {**existing, **meta_dict}
#     table = table.replace_schema_metadata(merged_meta)
    
#     with fs.open(path, 'wb') as f:
#         pq.write_table(table, f)
#     print(f"✅ Saved {path} with embedded metadata")

# def load_sq_config(path, con):
#     meta = pq.ParquetFile(path, filesystem=fs).metadata.metadata or {}
#     # meta = con.execute(f"""SELECT value FROM parquet_kv_metadata('{path}')""").fetchall()
#     return (
#         json.loads(meta[b"sq_config"].decode("utf-8"))
#         if b"sq_config" in meta
#         else None
#     )

# def visualize_from_parquet(path, fs):
#     try:
#         # Metadata-only read (no data scan)
#         with fs.open(path, 'rb') as f:
#             parquet_file = pq.ParquetFile(f)
#             raw_meta = parquet_file.metadata.metadata

#             if not raw_meta:
#                 print("No metadata found in file footer.")
#                 return None

#             # --- 1. Extract Data Quality Report ---
#             if b"dq_report" not in raw_meta:
#                 print("No DQ report found in file footer.")
#                 return None
            
#             report = json.loads(raw_meta[b"dq_report"].decode("utf-8"))
#             df_viz = pd.DataFrame(report["chart_data"])
            
#             print(f"Report Timestamp: {report.get('timestamp', 'Unknown')}")

#             # --- 2. Extract Config for Sorting (New Logic) ---
#             sort_order = None
#             if b"sq_config" in raw_meta:
#                 try:
#                     config = json.loads(raw_meta[b"sq_config"].decode("utf-8"))
#                     # Create the custom sort list from the 'target' fields in order
#                     sort_order = [item['target'] for item in config]
#                     # print(f"Applying custom sort order: {sort_order}")
#                 except Exception as ex:
#                     print(f"Warning: Could not load sort config: {ex}")
            
#             # --- 3. Generate Chart ---
#             chart = (
#                 alt.Chart(df_viz)
#                 .mark_bar()
#                 .encode(
#                     x=alt.X("Count:Q", stack="normalize", axis=alt.Axis(format="%", title="Percentage")),
                    
#                     # Apply the custom sort list here. 
#                     # If sort_order is None, it falls back to "ascending" (alphabetical)
#                     y=alt.Y("Column:N", sort=sort_order if sort_order else "ascending"),
                    
#                     color=alt.Color("Status:N", scale=alt.Scale(
#                         domain=["Passed", "Missing", "Invalid"], 
#                         range=["#2ca02c", "#ff7f0e", "#d62728"]
#                     )),
#                     # Force the stack order of the bars themselves
#                     order=alt.Order("Status", sort="descending"), 
#                     tooltip=[
#                         alt.Tooltip("Column:N", title="Field"),
#                         alt.Tooltip("Status:N", title="Status"),
#                         alt.Tooltip("Count:Q", title="Row Count"),
#                         alt.Tooltip("Top_Issues:N", title="Top 5 Values"),
#                     ],
#                 )
#                 .properties(
#                     title=f"Data Quality Report of {path.split('/')[-2]}",
#                     width=600,
#                     height=300,
#                 )
#             )
#             return chart

#     except Exception as e:
#         print(f"Error reading parquet metadata: {e}")
#         return None
