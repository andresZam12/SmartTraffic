# Microservicio 3 — Consumidor Kafka independiente: simula el archivado histórico de eventos.
# Demuestra que Kafka permite múltiples grupos leyendo el mismo topic sin interferirse:
# este servicio recibe TODOS los mensajes aunque el processor ya los haya consumido.

import json
import time
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC        = "raw_traffic_data"

def conectar_kafka():
    while True:
        try:
            consumidor = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                # "archiver_group" es diferente a "density_processor_group":
                # Kafka mantiene un offset independiente por grupo, así cada grupo
                # lee el topic desde su propio punto sin afectar al otro.
                group_id="archiver_group",
                auto_offset_reset="latest",
            )
            print(f"[ARCHIVER] Conectado a Kafka, escuchando '{TOPIC}'")
            return consumidor
        except NoBrokersAvailable:
            print("[ARCHIVER] Kafka no disponible, reintentando en 5s...")
            time.sleep(5)

def main():
    consumidor = conectar_kafka()
    contador   = 0
    print("[ARCHIVER] Iniciando archivo de datos brutos...")

    for mensaje in consumidor:
        evento   = mensaje.value
        contador += 1
        # En producción este print sería un INSERT a base de datos.
        # Se muestra partición y offset para demostrar el sistema de log de Kafka.
        print(
            f"[ARCHIVER] Archivando #{contador:04d} → "
            f"Zona: {evento['zone_id']}, "
            f"Vehículos: {evento['vehicle_count']}, "
            f"Partición: {mensaje.partition}, "
            f"Offset: {mensaje.offset}"
        )

if __name__ == "__main__":
    main()
