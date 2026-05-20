# Imagen base mínima: Python 3.11 (~130 MB vs ~900 MB de la versión completa)
FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python pika  # kafka: consume topic; pika: publica en RabbitMQ Fanout
COPY processor.py .
CMD ["python", "-u", "processor.py"]  # -u: logs sin buffer, visibles en docker logs
