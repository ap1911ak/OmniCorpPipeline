import requests
import os
import sqlite3
from pathlib import Path

def download_file(url, filename):
    """
    Downloads a file from a given URL and saves it with the specified filename.

    Args:
        url (str): The URL of the file to download.
        filename (str): The name to save the file as.
    """
    # Check if the file already exists to avoid re-downloading
    if os.path.exists(filename):
        print(f"'{filename}' already exists. Skipping download.")
        return

    print(f"Downloading {filename} from {url}...")
    try:
        # Use requests to get the file content. stream=True allows for large files.
        with requests.get(url, stream=True) as r:
            r.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            with open(filename, 'wb') as f:
                # Write the file content in chunks to save memory
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Successfully downloaded and saved '{filename}'.")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading '{filename}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def list_tables(db_file):
    """
    Connects to a SQLite database and prints a list of all tables.

    Args:
        db_file (str): The path to the SQLite database file.
    """
    print("-" * 30)
    print(f"Listing tables in '{db_file}':")
    try:
        # Connect to the database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Execute the query to find all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if tables:
            for table in tables:
                print(f"- {table[0]}")
        else:
            print("No tables found in this database.")

        # Close the connection
        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("-" * 30)

# Define the URLs for the databases. These are direct links to the raw files on GitHub.
CHINOOK_URL = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
# Updated URL for the Northwind database to resolve the 404 error
NORTHWIND_URL = "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"

# Define the desired filenames
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

# --- Main script execution ---

# Download the databases
download_file(CHINOOK_URL, CHINOOK_DB)
download_file(NORTHWIND_URL, NORTHWIND_DB)

print("\nAll download operations complete. Checking your current directory for the database files.")



