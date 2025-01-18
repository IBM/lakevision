FROM lakevision-base:latest
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app
WORKDIR /app/be
COPY ./be/requirements.txt ./
COPY ./be/app ./app
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY start.sh /app/start.sh
RUN chmod 755 /app/start.sh
#USER lv
WORKDIR /app/fe
COPY ./fe/ .

COPY nginx.conf /etc/nginx/

RUN rm -f package-lock.json && rm -rf node_modules
RUN npm install --package-lock-only
RUN npm ci
RUN npm install

EXPOSE 5173 8000 80
ENV HOST=0.0.0.0