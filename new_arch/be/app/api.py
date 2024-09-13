from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Union
from lakeviewer import LakeView

app = FastAPI()
lv = LakeView()
namespaces = lv.get_namespaces()
origins = [
    "http://localhost",
    "http://localhost:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/namespaces")
def read_namespaces():    
    ret = []
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

@app.get("/tables")
def read_tables(namespace: str = None):    
    ret = []
    if not namespace:
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1]})        
    return ret
    

    

