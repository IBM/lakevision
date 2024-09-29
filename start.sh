#! /bin/bash
cd /fe
npm run dev -- --host&
#fastapi run /be/app/api.py --port 8000
cd /be/app/
gunicorn -k uvicorn.workers.UvicornWorker api:app --bind 0.0.0.0:8000 --workers 2 --threads 32

