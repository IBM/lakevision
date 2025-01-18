#! /bin/bash
cd /app/be/app/
fastapi run /app/be/app/api.py --port 8000 &
#gunicorn -k uvicorn.workers.UvicornWorker api:app --bind 0.0.0.0:8000 --workers 2 --threads 32
cd /app/fe
npm run build
node build &
nginx -g 'daemon off;'
#npm run dev -- --host&


