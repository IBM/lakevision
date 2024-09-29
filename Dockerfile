FROM lakevision-base:latest
ENV PYTHONUNBUFFERED=1
WORKDIR /be
COPY ./be/requirements.txt /be/
COPY ./be/app /be/app
RUN pip install --no-cache-dir --upgrade -r /be/requirements.txt

#USER lv
WORKDIR /fe

COPY ./fe/ .
COPY start.sh /
RUN chmod 755 /start.sh
RUN rm package-lock.json && rm -r node_modules
RUN npm install --package-lock-only
RUN npm ci
RUN npm install

EXPOSE 5173 8000
ENV HOST=0.0.0.0
CMD [/start.sh]
