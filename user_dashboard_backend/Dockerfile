# Imagen base: Python 3.11 slim.
FROM python:3.11-slim

WORKDIR /app

# Este microservicio necesita DOS librerías externas:
# pika:       cliente AMQP para consumir el Fanout Exchange de RabbitMQ.
# websockets: servidor WebSocket para enviar datos al navegador en tiempo real.
# Nota: el servidor HTTP REST usa http.server de la librería estándar — no requiere instalación.
RUN pip install pika websockets

COPY dashboard_backend.py .

CMD ["python", "-u", "dashboard_backend.py"]
