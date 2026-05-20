# Imagen base: Python 3.11 slim — mínimo necesario para correr Python sin extras.
FROM python:3.11-slim

WORKDIR /app

# Este microservicio necesita DOS librerías porque actúa en ambos brokers:
# kafka-python: consume el topic raw_traffic_data desde Kafka.
# pika:         publica el estado procesado en el Fanout Exchange de RabbitMQ.
RUN pip install kafka-python pika

COPY processor.py .

# -u: logs sin buffer — esencial para ver los broadcasts en tiempo real con docker logs.
CMD ["python", "-u", "processor.py"]
