# Imagen base: Python 3.11 slim.
FROM python:3.11-slim

WORKDIR /app

# Solo kafka-python: este microservicio únicamente consume de Kafka.
# No usa RabbitMQ — demuestra que los consumer groups son completamente independientes:
# el archiver lee el mismo topic que el processor sin ninguna dependencia entre ellos.
RUN pip install kafka-python

COPY archiver.py .

CMD ["python", "-u", "archiver.py"]
