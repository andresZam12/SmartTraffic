# Microservicio 2 — Cerebro del sistema: consume Kafka, calcula densidad por zona (patrón KTable)
# y cada 5 segundos hace broadcast del estado completo al Fanout Exchange de RabbitMQ.

import json
import time
import os
import threading
from collections import deque
import pika
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER    = os.environ.get("KAFKA_BROKER", "localhost:9092")
RABBITMQ_HOST   = os.environ.get("RABBITMQ_HOST", "localhost")
TOPIC           = "raw_traffic_data"
FANOUT_EXCHANGE = "traffic_updates"
BROADCAST_CADA  = 5   # segundos entre cada snapshot publicado en RabbitMQ
VENTANA         = 5   # cuántos eventos recientes se promedian por zona
UMBRAL_CONGESTIONADA = 70
UMBRAL_MODERADA      = 40

# KTable en memoria: zone_id → estado actual ("DESPEJADA" / "MODERADA" / "CONGESTIONADA")
zone_states = {}

# Ventana deslizante por zona: guarda solo los últimos VENTANA conteos.
# Usar deque(maxlen=N) descarta automáticamente el valor más antiguo al insertar el nuevo.
# Esto evita que el promedio acumulado converja siempre a ~50 (MODERADA) con datos aleatorios.
zone_ventana = {}

def clasificar(promedio):
    # Convierte el promedio numérico en una etiqueta de estado.
    if promedio >= UMBRAL_CONGESTIONADA:
        return "CONGESTIONADA"
    elif promedio >= UMBRAL_MODERADA:
        return "MODERADA"
    return "DESPEJADA"

def conectar_rabbitmq():
    # Abre una conexión + declara el Fanout Exchange. Reintenta si RabbitMQ no está listo.
    while True:
        try:
            conexion = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            canal = conexion.channel()
            # durable=True: el exchange sobrevive a un reinicio de RabbitMQ.
            canal.exchange_declare(exchange=FANOUT_EXCHANGE, exchange_type="fanout", durable=True)
            print(f"[PROCESSOR] Conectado a RabbitMQ en {RABBITMQ_HOST}")
            return conexion, canal
        except Exception:
            print("[PROCESSOR] RabbitMQ no disponible, reintentando en 5s...")
            time.sleep(5)

def conectar_kafka():
    # Crea un consumidor en el grupo "density_processor_group".
    # Cada grupo recibe una copia completa del topic independientemente del resto.
    while True:
        try:
            consumidor = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="density_processor_group",
                auto_offset_reset="latest",
            )
            print(f"[PROCESSOR] Conectado a Kafka, escuchando '{TOPIC}'")
            return consumidor
        except NoBrokersAvailable:
            print("[PROCESSOR] Kafka no disponible, reintentando en 5s...")
            time.sleep(5)

def hilo_broadcast():
    # Este hilo corre en paralelo al loop de Kafka y publica el estado cada BROADCAST_CADA segundos.
    # Crea su PROPIA conexión pika porque pika.BlockingConnection no es thread-safe:
    # usarla desde otro hilo produce errores de frame o conexión perdida.
    _, canal = conectar_rabbitmq()

    while True:
        time.sleep(BROADCAST_CADA)

        if not zone_states:
            print("[PROCESSOR] Sin datos aún, esperando...")
            continue

        # Copias locales de los dicts antes de iterar para evitar RuntimeError
        # si el hilo principal los modifica mientras se construye el snapshot.
        states_copy  = dict(zone_states)
        ventana_copy = {z: list(v) for z, v in zone_ventana.items()}

        snapshot = {
            zona: {
                "estado": estado,
                "promedio": round(sum(ventana_copy[zona]) / len(ventana_copy[zona]), 1)
            }
            for zona, estado in states_copy.items()
            if zona in ventana_copy and ventana_copy[zona]
        }

        try:
            # En un Fanout Exchange la routing_key se ignora;
            # RabbitMQ entrega el mensaje a TODAS las colas ligadas al exchange.
            canal.basic_publish(exchange=FANOUT_EXCHANGE, routing_key="", body=json.dumps(snapshot))
            print(f"[PROCESSOR] Broadcast enviado → {snapshot}")
        except Exception as e:
            print(f"[PROCESSOR] Error publicando: {e}")
            _, canal = conectar_rabbitmq()  # reconectar y retomar en el próximo ciclo

def main():
    consumidor_kafka = conectar_kafka()

    # daemon=True: el hilo de broadcast muere automáticamente si el proceso principal termina.
    threading.Thread(target=hilo_broadcast, daemon=True).start()

    print("[PROCESSOR] Procesando eventos de Kafka...")

    for mensaje in consumidor_kafka:
        zona   = mensaje.value["zone_id"]
        conteo = mensaje.value["vehicle_count"]

        # Inicializar la ventana de la zona la primera vez que aparece.
        if zona not in zone_ventana:
            zone_ventana[zona] = deque(maxlen=VENTANA)
        zone_ventana[zona].append(conteo)

        promedio        = sum(zone_ventana[zona]) / len(zone_ventana[zona])
        estado_anterior = zone_states.get(zona)
        estado_nuevo    = clasificar(promedio)

        # Solo imprime cuando el estado cambia para no saturar la consola.
        if estado_nuevo != estado_anterior:
            print(f"[PROCESSOR] {zona}: {estado_anterior} → {estado_nuevo} (prom: {promedio:.1f})")

        zone_states[zona] = estado_nuevo

if __name__ == "__main__":
    main()
