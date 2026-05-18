# Microservicio 1 — Productor Kafka: genera eventos de tráfico aleatorios cada segundo.

import json
import time
import random
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC        = "raw_traffic_data"
ZONAS        = ["Zona A", "Zona B", "Zona C", "Zona D", "Zona E"]
INTERVALO    = 1  # segundos entre eventos

def conectar_kafka():
    # Reintenta indefinidamente hasta que Kafka esté listo (puede tardar al arrancar).
    while True:
        try:
            productor = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # La key es el zone_id: Kafka garantiza que todos los mensajes
                # con la misma key van a la misma partición, preservando el orden por zona.
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print(f"[SENSOR] Conectado a Kafka en {KAFKA_BROKER}")
            return productor
        except NoBrokersAvailable:
            print("[SENSOR] Kafka no disponible, reintentando en 5s...")
            time.sleep(5)

def generar_evento():
    # Elige una zona al azar y genera un conteo de vehículos entre 0 y 100.
    zona = random.choice(ZONAS)
    return zona, {"zone_id": zona, "vehicle_count": random.randint(0, 100)}

def main():
    productor = conectar_kafka()
    print(f"[SENSOR] Publicando en topic '{TOPIC}' cada {INTERVALO}s...")

    while True:
        zona, evento = generar_evento()
        # send() encola el mensaje; flush() lo fuerza a enviarse antes de dormir.
        productor.send(TOPIC, key=zona, value=evento)
        productor.flush()
        print(f"[SENSOR] Publicado → {evento}")
        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()
