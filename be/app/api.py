from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from lakeviewer import LakeView
from typing import Generator
from pyiceberg.table import Table
import time
from threading import Timer

app = FastAPI()
lv = LakeView()

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

page_session_cache = {}
CACHE_EXPIRATION = 4 * 60 # 4 minutes
namespaces = lv.get_namespaces()

def load_table(table_id: str) -> Table: #Generator[Table, None, None]:    
    try:
        print(f"Loading table {table_id}")
        table = lv.load_table(table_id)
        return table  # This makes it a generator
    except Exception as e:
        raise HTTPException(status_code=404, detail="Table not found")
    finally:
        # Optional cleanup
        print(f"Finished with loading table {table_id}")

# Dependency to load the table only once
def get_table(request: Request, table_id: str):
    page_session_id = request.headers.get("X-Page-Session-ID")    
    if not page_session_id:
        raise HTTPException(status_code=400, detail="Missing X-Page-Session-ID header")
    cache_key = f"{page_session_id}_{table_id}"

    if cache_key not in page_session_cache:
        tbl = load_table(table_id)
        page_session_cache[cache_key] = (tbl, time.time())
    else:
        tbl, timestamp = page_session_cache[cache_key]
    return tbl    

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/namespaces")
def read_namespaces(refresh=False):    
    ret = []
    global namespaces
    if refresh or len(namespaces)==0:
        namespaces = lv.get_namespaces()
    for idx, ns in enumerate(namespaces):
        ret.append({"id": idx, "text": ".".join(ns)})        
    return ret

@app.get("/tables/{table_id}/snapshots")
def read_table_snapshots(table: Table = Depends(get_table)):
    return lv.get_snapshot_data(table)

@app.get("/tables/{table_id}/partitions")
def read_table_partitions(table: Table = Depends(get_table)):
    return lv.get_partition_data(table)

@app.get("/tables/{table_id}/sample")    
def read_sample_data(table: Table = Depends(get_table), partition=None, limit=100):
    return lv.get_sample_data(table, partition, limit)

@app.get("/tables/{table_id}/schema")    
def read_schema_data(table: Table = Depends(get_table)):
    return lv.get_schema(table)

@app.get("/tables/{table_id}/summary")    
def read_summary_data(table: Table = Depends(get_table)):
    return lv.get_summary(table)

@app.get("/tables/{table_id}/properties")    
def read_properties_data(table: Table = Depends(get_table)):
    return lv.get_properties(table)

@app.get("/tables/{table_id}/partition-specs")    
def read_partition_specs(table: Table = Depends(get_table)):
    return lv.get_partition_specs(table)

@app.get("/tables/{table_id}/data-change")    
def read_data_change(table: Table = Depends(get_table)):
    return lv.get_data_change(table)

@app.get("/tables")
def read_tables(namespace: str = None):    
    ret = []
    if not namespace:
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1]})        
    return ret
    
def clean_cache():
    """Remove expired entries from the cache."""
    current_time = time.time()
    keys_to_delete = [
        key for key, (_, timestamp) in page_session_cache.items()
        if current_time - timestamp > CACHE_EXPIRATION
    ]
    for key in keys_to_delete:
        del page_session_cache[key]

# Schedule periodic cleanup every minute
def schedule_cleanup():
    clean_cache()
    Timer(60, schedule_cleanup).start()

schedule_cleanup()
    

