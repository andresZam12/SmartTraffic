# Imagen base mínima: Python 3.11 (~130 MB vs ~900 MB de la versión completa)
FROM python:3.11-slim
WORKDIR /app
RUN pip install pika  # solo RabbitMQ: Fanout + Work Queue + Topic Exchange
COPY dispatcher.py .
CMD ["python", "-u", "dispatcher.py"]  # -u: logs sin buffer, visibles en docker logs
