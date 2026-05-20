# Imagen base mínima: Python 3.11 (~130 MB vs ~900 MB de la versión completa)
FROM python:3.11-slim
WORKDIR /app
RUN pip install pika  # solo RabbitMQ: Work Queue (consultas) + Topic Exchange (respuestas)
COPY client.py .
CMD ["python", "-u", "client.py"]  # -u: logs sin buffer, visibles en docker logs
