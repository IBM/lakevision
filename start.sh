#! /bin/bash
cd /be/app/
fastapi run /be/app/api.py --port 8000 &
#gunicorn -k uvicorn.workers.UvicornWorker api:app --bind 0.0.0.0:8000 --workers 2 --threads 32
cd /fe
npm run build
node build
#npm run dev -- --host&


