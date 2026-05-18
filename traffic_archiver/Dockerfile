FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python
COPY archiver.py .
CMD ["python", "-u", "archiver.py"]
