from fastapi import FastAPI, Depends, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import JSONResponse
from lakeviewer import LakeView
from typing import Generator
from pyiceberg.table import Table
import time, os, requests
from threading import Timer
from pydantic import BaseModel
from authz import Authz

AUTH_ENABLED        = True if os.getenv("PUBLIC_AUTH_ENABLED", '')=='true' else False
CLIENT_ID           = os.getenv("PUBLIC_OPENID_CLIENT_ID", '')
OPENID_PROVIDER_URL = os.getenv("PUBLIC_OPENID_PROVIDER_URL", '')
REDIRECT_URI        = os.getenv("PUBLIC_REDIRECT_URI", '')
CLIENT_SECRET       = os.getenv("OPEN_ID_CLIENT_SECRET", '')
TOKEN_URL           = OPENID_PROVIDER_URL+"/token"
SECRET_KEY          = os.getenv("SECRET_KEY", "@#dsfdds1112")

app = FastAPI()
lv = LakeView()

app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = Config(environ={
    'AUTHLIB_INSECURE_TRANSPORT': '1',  # Only for local development; remove in production
})
oauth = OAuth(config)

oauth.register(
    name="openid",
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    server_metadata_url=f"{OPENID_PROVIDER_URL}/.well-known/openid-configuration",
    client_kwargs={
        "scope": "email"
    }
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
    if AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            raise HTTPException(status_code=503, detail="User Not logged in")
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

def check_auth(request: Request):
    user = request.session.get("user")
    if user:
        return JSONResponse(user)
    return None

@app.get("/api/namespaces")
def read_namespaces(refresh=False):    
    ret = []
    global namespaces
    if refresh or len(namespaces)==0:
        namespaces = lv.get_namespaces()
    for idx, ns in enumerate(namespaces):
        ret.append({"id": idx, "text": ".".join(ns)})        
    return ret

@app.get("/api/tables/{table_id}/snapshots")
def read_table_snapshots(table: Table = Depends(get_table)):
    return lv.get_snapshot_data(table)

@app.get("/api/tables/{table_id}/partitions", status_code=status.HTTP_200_OK)
def read_table_partitions(request: Request, response: Response, table: Table = Depends(get_table)):
    az = Authz(request, response)
    if not az.has_access(table):        
        return
    return lv.get_partition_data(table)

@app.get("/api/tables/{table_id}/sample", status_code=status.HTTP_200_OK)    
def read_sample_data(request: Request, response: Response, table: Table = Depends(get_table), partition=None, sample_limit=100):
    az = Authz(request, response)
    if not az.has_access(table):        
        return
    return lv.get_sample_data(table, partition, sample_limit)

@app.get("/api/tables/{table_id}/schema")    
def read_schema_data(table: Table = Depends(get_table)):
    return lv.get_schema(table)

@app.get("/api/tables/{table_id}/summary")    
def read_summary_data(table: Table = Depends(get_table)):
    return lv.get_summary(table)

@app.get("/api/tables/{table_id}/properties")    
def read_properties_data(table: Table = Depends(get_table)):
    return lv.get_properties(table)

@app.get("/api/tables/{table_id}/partition-specs")    
def read_partition_specs(table: Table = Depends(get_table)):
    return lv.get_partition_specs(table)

@app.get("/api/tables/{table_id}/data-change")    
def read_data_change(table: Table = Depends(get_table)):
    return lv.get_data_change(table)

@app.get("/")
def root(request: Request):
    if AUTH_ENABLED:
        user = check_auth(request)
        if not user:
            return {"message": "You are not logged in."}
    return "Hello, no auth enabled"
       
@app.get("/api/tables")
def read_tables(namespace: str = None, user=Depends(check_auth)):    
    if AUTH_ENABLED and not user:
        return RedirectResponse("/")
    ret = []
    if not namespace:
        return ret
    for idx, table in enumerate(lv.get_tables(namespace)):
        ret.append({"id": idx, "text": table[-1]})        
    return ret

@app.get("/api/login")
async def login(request: Request):
    redirect_uri = REDIRECT_URI
    return await oauth.openid.authorize_redirect(request, redirect_uri)

class TokenRequest(BaseModel):
    code: str

@app.post("/api/auth/token")
def get_token(request: Request, tokenReq: TokenRequest):
    data = {
        "grant_type": "authorization_code",
        "code": tokenReq.code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }    
    response = requests.post(TOKEN_URL, data=data)
    #print(response.json())
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to exchange code for token")
    r2 = requests.get(f"{OPENID_PROVIDER_URL}/userinfo?access_token={response.json()['access_token']}")
    user_email = r2.json()['email'] 
    request.session['user'] = user_email
    return JSONResponse(user_email)  

@app.get("/api/logout")
def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(REDIRECT_URI)

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
    

