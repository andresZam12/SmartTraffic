FROM python:3.11-slim
WORKDIR /app
RUN pip install pika
COPY dispatcher.py .
CMD ["python", "-u", "dispatcher.py"]
