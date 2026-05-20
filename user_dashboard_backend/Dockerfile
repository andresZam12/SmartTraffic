# Imagen base mínima: Python 3.11 (~130 MB vs ~900 MB de la versión completa)
FROM python:3.11-slim
WORKDIR /app
RUN pip install pika websockets  # pika: consume RabbitMQ; websockets: push al navegador
COPY dashboard_backend.py .
CMD ["python", "-u", "dashboard_backend.py"]  # -u: logs sin buffer, visibles en docker logs
