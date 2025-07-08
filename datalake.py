import json
import shutil
import subprocess
from pathlib import Path
import duckdb as ddb
import re

# === Configuration ===
DATA_LAKE_ROOT = Path("my-datalake")
TABLES_DIR = DATA_LAKE_ROOT / "tables"
METADATA_DIR = DATA_LAKE_ROOT / "metadata"
CATALOG_FILE = METADATA_DIR / "catalog.json"
# === Setup ===
def ensure_structure():
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    if not CATALOG_FILE.exists():
        CATALOG_FILE.write_text("{}")


def load_catalog() -> dict:
    return json.loads(CATALOG_FILE.read_text())

def save_catalog(catalog: dict):
    CATALOG_FILE.write_text(json.dumps(catalog, indent=2))


# === Schema Inference using DuckDB ===
def infer_schema(file_path: Path, file_format: str = "csv") -> tuple[dict, int]:
    
    if file_format == "csv":
        rel = ddb.read_csv(file_path)
    elif file_format == "parquet":
        rel = ddb.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

    # Get column names and types directly from relation
    schema = {col: str(typ) for col, typ in zip(rel.columns, rel.types)}

    return schema


# === Git Integration ===
def git_commit(message: str):
    try:
        subprocess.run(["git", "add", "."], cwd=DATA_LAKE_ROOT, check=True)
        subprocess.run(["git", "commit", "-m", message], cwd=DATA_LAKE_ROOT, check=True)
        print(f"📦 Git commit: {message}")
    except subprocess.CalledProcessError:
        print("⚠️ Git commit failed (maybe repo not initialized?)")

# === Add Table ===
def add_table(logical_name: str, source_file_path: str, description_path: str = None):
    ensure_structure()
    catalog = load_catalog()

    if logical_name in catalog:
        raise ValueError(f"Table '{logical_name}' already exists.")
    
    table_index = len(catalog)

    # Detect file format
    src_path = Path(source_file_path)
    ext = src_path.suffix.lower()
    if ext == ".csv":
        file_format = "csv"
    elif ext == ".parquet":
        file_format = "parquet"
    else:
        raise ValueError("Unsupported file type. Only .csv and .parquet are supported.")

    # Copy and rename table file
    dst_path = TABLES_DIR / f"{table_index}{ext}"
    shutil.copy(src_path, dst_path)

    # Copy and rename description file (optional)
    desc_dst = None
    if description_path:
        desc_src = Path(description_path)
        desc_dst = TABLES_DIR / f"{table_index}.description.md"
        shutil.copy(desc_src, desc_dst)

    # Infer schema (without loading full file)
    schema, _ = infer_schema(dst_path, file_format)

    # Update catalog entry
    catalog[logical_name] = {
        "index": table_index,
        "file": f"tables/{table_index}{ext}",
        "description_file": f"tables/{table_index}.description.md" if desc_dst else None,
        "format": file_format,
        "schema": schema
    }

    # Add to catalog.json
    save_catalog(catalog)


    print(f"✅ Added table '{logical_name}' as index {table_index}")
    print(f"   File: {dst_path.name}")
    if desc_dst:
        print(f"   Description: {desc_dst.name}")

    # Git commit
    git_commit(f"Add table: {logical_name}")

# === List Tables ===
def list_tables():
    ensure_structure()
    catalog = load_catalog()

    index = []
    table_names = []
    for name in catalog.keys():
        table_names.append(name)
        index.append(catalog[name]["index"])

    # Print as two columns
    print(f"{'Index':<10} {'Table Name'}")
    print("-" * 30)
    for idx, name in zip(index, table_names):
        print(f"{idx:<10} {name}")

    print("This only show the first 10 table. Please check catalog.json for more table's informaiton")
    

# === Get metadata ===
def get_table_meta(logical_name: str) -> dict:
    catalog = load_catalog()
    entry = catalog.get(logical_name)

    if not entry:
        raise ValueError(f"Table '{logical_name}' not found")

    
    return entry

# === Query data from tables === 
def query_sql(sql: str):
    """
    Querying data from tables by SQL command
    e.g: "SELECT plugin_time FROM norway_evc WHERE plugin_time > "2025-10-01"

    Args:
        sql (str): sql query
    
    Return:
        conn (duckdb.duckdb.DuckDBPyConnection object): connection to duckdb
        rel (QUERY_RELATION): relation objection of duckdb, which a lazy evaluation
    """
    ensure_structure()
    catalog = load_catalog()

    # Extract tokens
    tokens = set(re.findall(r'\b\w+\b', sql))

    # Compute fast intersection
    matched_names = set(catalog.keys()).intersection(tokens)

    # Replace only matched names
    for name in matched_names:
        meta = catalog[name]
        file_path = (DATA_LAKE_ROOT / meta["file"]).as_posix()
        sql = re.sub(rf'\b{name}\b', f"'{file_path}'", sql)

    print("▶️ Rewritten SQL:", sql)

    conn = ddb.connect()
    rel = conn.query(sql)
    return conn, rel