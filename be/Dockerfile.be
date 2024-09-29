FROM python:3.12.5
WORKDIR /be
COPY requirements.txt /be/
COPY app /be/app
RUN pip install --no-cache-dir --upgrade -r /be/requirements.txt

CMD ["fastapi", "run", "/be/app/api.py", "--port", "8000"]
