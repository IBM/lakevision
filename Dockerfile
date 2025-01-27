FROM node:23.6.0-bookworm AS builder
RUN \
  apt-get update && \
  #apt-get update --allow-insecure-repositories --allow-unauthenticated && \
  apt-get install -y nginx && \
  apt-get install -y python3 python3-pip && \
  rm /usr/lib/python3.*/EXTERNALLY-MANAGED && \
  pip install -U pip pipenv uv && \
  #curl -sSL https://install.python-poetry.org | python3 - && \
  rm -rf /var/lib/apt/lists/*
RUN pip install pyiceberg s3fs fastapi[standard] pandas uvicorn pyarrow

FROM builder AS be
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app
WORKDIR /app/be
COPY ./be/requirements.txt ./
COPY ./be/app ./app
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY start.sh /app/start.sh
RUN chmod 755 /app/start.sh
#USER lv

FROM be
WORKDIR /app/fe
COPY ./fe/ .

COPY nginx.conf /etc/nginx/

RUN rm -f package-lock.json && rm -rf node_modules
RUN npm install --package-lock-only
RUN npm ci
RUN npm install

EXPOSE 5173 8000 80
ENV HOST=0.0.0.0