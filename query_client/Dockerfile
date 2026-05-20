# Imagen base: Python 3.11 slim.
FROM python:3.11-slim

WORKDIR /app

# Solo pika: publica consultas en la Work Queue y escucha respuestas
# en el Topic Exchange, ambos en RabbitMQ. Implementa el patrón
# Request/Reply asíncrono sin bloquear el hilo principal.
RUN pip install pika

COPY client.py .

CMD ["python", "-u", "client.py"]
