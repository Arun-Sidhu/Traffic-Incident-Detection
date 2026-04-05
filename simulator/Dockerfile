# simulator/Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN pip install --no-cache-dir kafka-python psycopg2-binary

COPY gps_ping_producer.py /app/gps_ping_producer.py
COPY postgres_sink.py /app/postgres_sink.py

CMD ["python", "gps_ping_producer.py"]