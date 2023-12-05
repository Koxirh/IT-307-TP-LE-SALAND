FROM python:3

ENV PYTHON_UNBUFFERED = 1

RUN pip install minio pika

COPY ./src/1_rabbit_to_minio.py ./

ENTRYPOINT python3 -u ./1_rabbit_to_minio.py
