# Imagen base: Python 3.11 slim.
FROM python:3.11-slim

WORKDIR /app

# Solo pika: este microservicio opera exclusivamente en RabbitMQ.
# Escucha el Fanout Exchange (estado de zonas) y la Work Queue (consultas de usuarios),
# y publica respuestas en el Topic Exchange. Todo via pika con dos conexiones separadas
# porque pika no es thread-safe.
RUN pip install pika

COPY dispatcher.py .

CMD ["python", "-u", "dispatcher.py"]
