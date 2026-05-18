FROM python:3.11-slim
WORKDIR /app
RUN pip install pika websockets
COPY dashboard_backend.py .
CMD ["python", "-u", "dashboard_backend.py"]
