FROM apache/airflow:3.1.1

ENV TZ=America/Sao_Paulo

COPY ./requirements.txt /requirements.txt

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r /requirements.txt