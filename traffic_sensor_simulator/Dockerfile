# Imagen base: Python 3.11 en variante "slim" (sin herramientas de compilación ni docs).
# slim reduce el tamaño del contenedor de ~900 MB a ~130 MB — importante en producción.
FROM python:3.11-slim

# Establece el directorio de trabajo dentro del contenedor.
# Todos los comandos siguientes (COPY, CMD) operan desde /app.
WORKDIR /app

# Instala SOLO las dependencias necesarias para este microservicio.
# kafka-python: librería para conectarse a Kafka como productor.
# No se instala pika porque este servicio no usa RabbitMQ.
RUN pip install kafka-python

# Copia únicamente el script del microservicio al contenedor.
# No se copia todo el proyecto — cada contenedor tiene solo lo que necesita.
COPY sensor.py .

# Comando que se ejecuta al arrancar el contenedor.
# -u: modo "unbuffered" — los print() aparecen inmediatamente en los logs de Docker
#     sin esperar a que se llene el buffer. Sin -u, docker compose logs no mostraría nada.
CMD ["python", "-u", "sensor.py"]
