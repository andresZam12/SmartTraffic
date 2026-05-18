FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python pika
COPY processor.py .
CMD ["python", "-u", "processor.py"]
