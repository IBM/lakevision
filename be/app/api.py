from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from lakeviewer import LakeView

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

@app.get("/")
def read_root():
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
def read_table_snapshots(table_id: str = None):       
    return lv.get_snapshot_data(table_id)

@app.get("/tables/{table_id}/partitions")
def read_table_partitions(table_id: str = None):       
    return lv.get_partition_data(table_id)

@app.get("/tables/{table_id}/sample")    
def read_sample_data(table_id: str, partition=None, limit=100):
    return lv.get_sample_data(table_id, partition, limit)

@app.get("/tables/{table_id}/schema")    
def read_schema_data(table_id: str):
    return lv.get_schema(table_id)

@app.get("/tables/{table_id}/summary")    
def read_summary_data(table_id: str):
    return lv.get_summary(table_id)

@app.get("/tables/{table_id}/properties")    
def read_properties_data(table_id: str):
    return lv.get_properties(table_id)

@app.get("/tables/{table_id}/partition-specs")    
def read_partition_specs(table_id: str):
    return lv.get_partition_specs(table_id)

@app.get("/tables/{table_id}/data-change")    
def read_data_change(table_id: str):
    return lv.get_data_change(table_id)

@app.get("/tables")
def read_tables(namespace: str = None):    
    ret = []
    if not namespace:
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1]})        
    return ret
    

    

