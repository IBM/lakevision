from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from lakeviewer import LakeView
from typing import Generator
from pyiceberg.table import Table

app = FastAPI()
lv = LakeView()

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
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
        print(f"Finished with table {table_id}")

page_session_cache = {}

# Dependency to load the table only once
def get_table(request: Request, table_id: str):
    page_session_id = request.headers.get("X-Page-Session-ID")
    print(page_session_id)
    if not page_session_id:
        raise HTTPException(status_code=400, detail="Missing X-Page-Session-ID header")
    cache_key = f"{page_session_id}_{table_id}"

    if cache_key not in page_session_cache:
        page_session_cache[cache_key] = load_table(table_id)
    return page_session_cache[cache_key]    

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/namespaces")
async def read_namespaces(refresh=False):    
    ret = []
    global namespaces
    if refresh or len(namespaces)==0:
        namespaces = lv.get_namespaces()
    for idx, ns in enumerate(namespaces):
        ret.append({"id": idx, "text": ".".join(ns)})        
    return ret

@app.get("/tables/{table_id}/snapshots")
async def read_table_snapshots(table: Table = Depends(get_table)):
    return lv.get_snapshot_data(table)

@app.get("/tables/{table_id}/partitions")
async def read_table_partitions(table: Table = Depends(get_table)):
    return lv.get_partition_data(table)

@app.get("/tables/{table_id}/sample")    
async def read_sample_data(table: Table = Depends(get_table), partition=None, limit=100):
    return lv.get_sample_data(table, partition, limit)

@app.get("/tables/{table_id}/schema")    
async def read_schema_data(table: Table = Depends(get_table)):
    return lv.get_schema(table)

@app.get("/tables/{table_id}/summary")    
async def read_summary_data(table: Table = Depends(get_table)):
    return lv.get_summary(table)

@app.get("/tables/{table_id}/properties")    
async def read_properties_data(table: Table = Depends(get_table)):
    return lv.get_properties(table)

@app.get("/tables/{table_id}/partition-specs")    
async def read_partition_specs(table: Table = Depends(get_table)):
    return lv.get_partition_specs(table)

@app.get("/tables/{table_id}/data-change")    
async def read_data_change(table: Table = Depends(get_table)):
    return lv.get_data_change(table)

@app.get("/tables")
async def read_tables(namespace: str = None):    
    ret = []
    if not namespace:
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1]})        
    return ret
    

    

