FROM python:3.11-slim
WORKDIR /app
RUN pip install pika
COPY client.py .
CMD ["python", "-u", "client.py"]
