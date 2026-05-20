# Imagen base mínima: Python 3.11 (~130 MB vs ~900 MB de la versión completa)
FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python  # solo Kafka — consumer group independiente del processor
COPY archiver.py .
CMD ["python", "-u", "archiver.py"]  # -u: logs sin buffer, visibles en docker logs
