FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt /app/
RUN pip3 install -r requirements.txt
COPY *.py /app/
COPY table_tabs/*.py /app/table_tabs/
COPY *.css /app/
EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "lake_viewer.py", "--server.port=8501", "--server.address=0.0.0.0"]
