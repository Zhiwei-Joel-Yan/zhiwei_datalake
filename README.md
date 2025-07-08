# **this is steps how to build your own datalake**

## datalake structure
my-datalake/
├── tables/
│   ├── 0.csv
│   ├── 0.description.md
│   ├── 1.csv
│   └── ...
├── metadata/
│   ├── catalog.json
│   └── table_names.json

The structure lies on the logic that stores and metainfo including schema and index of table & description in a json file, and save tables in a separated table.
Currently csv and parquet files are supported, it would be integrating other tabular files and image/textual files.

## ingestion [python]
The ingestion work is done by python with dependencies on duckdb for lazy reading the table.
Basic steps in designed as below
1. Check the folder structure. Create one if any is missing.
2. Read the table and get columns and schema.
3. Indexing the table and rename it togehter with the desciption file. 
4. Copy them to the destinated table folder.
5. Write metadata to "catalog.json"
6. Add and commit this change in git for version control.

### how to ingest a new table to the lake
call the "add_table" function from "datalake.py" package. The funciton requires a logical name of the table, path to the source file and description file
e.g. 
"""
from datalake import add_table
add_table("a_new_table", source_file_path="~/new_table.csv", description_path="~/description_new_table.md")
"""


