# Imagen base mínima: Python 3.11 (~130 MB vs ~900 MB de la versión completa)
FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python  # solo Kafka — este servicio no usa RabbitMQ
COPY sensor.py .
CMD ["python", "-u", "sensor.py"]  # -u: logs sin buffer, visibles en docker logs
