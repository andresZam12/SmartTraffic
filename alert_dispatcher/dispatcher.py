# Microservicio 5 — Dispatcher: replica el estado de las zonas y responde consultas de usuarios.
# Tiene dos responsabilidades corriendo en paralelo:
#   1. Escuchar el Fanout para mantener una copia local del estado (estado replicado).
#   2. Escuchar la Work Queue para responder consultas usando ese estado local.

import json
import time
import os
import threading
import pika

RABBITMQ_HOST   = os.environ.get("RABBITMQ_HOST", "localhost")
FANOUT_EXCHANGE = "traffic_updates"
WORK_QUEUE      = "query_traffic_queue"
TOPIC_EXCHANGE  = "query_answers"

# Copia local del estado de las zonas, actualizada cada vez que llega un broadcast.
# Permite responder consultas sin depender del processor en tiempo real.
zone_states_local = {}

def conectar_rabbitmq():
    while True:
        try:
            conexion = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print(f"[DISPATCHER] Conectado a RabbitMQ en {RABBITMQ_HOST}")
            return conexion
        except Exception:
            print("[DISPATCHER] RabbitMQ no disponible, reintentando en 5s...")
            time.sleep(5)

def hilo_fanout(conexion):
    # Hilo 1: mantiene zone_states_local actualizado recibiendo cada broadcast del processor.
    canal = conexion.channel()
    canal.exchange_declare(exchange=FANOUT_EXCHANGE, exchange_type="fanout", durable=True)

    # Cola exclusiva y anónima: solo este servicio la usa, desaparece al desconectarse.
    nombre_cola = canal.queue_declare(queue="", exclusive=True).method.queue
    canal.queue_bind(exchange=FANOUT_EXCHANGE, queue=nombre_cola)

    def actualizar_estado(ch, method, properties, body):
        global zone_states_local
        nuevo_estado    = json.loads(body)
        # Extraemos el estado anterior para detectar cambios zona a zona.
        estado_anterior = {z: d["estado"] for z, d in zone_states_local.items()}
        zone_states_local = nuevo_estado

        # Alerta proactiva: imprime solo las zonas cuyo estado cambió.
        for zona, datos in nuevo_estado.items():
            if datos["estado"] != estado_anterior.get(zona):
                print(f"[DISPATCHER] ALERTA: {zona} ahora está {datos['estado']}!")

    print(f"[DISPATCHER] Escuchando Fanout '{FANOUT_EXCHANGE}'...")
    canal.basic_consume(queue=nombre_cola, on_message_callback=actualizar_estado, auto_ack=True)
    canal.start_consuming()

def hilo_work_queue(conexion):
    # Hilo 2: procesa preguntas de usuarios y envía respuestas al cliente correcto.
    canal = conexion.channel()
    # Topic Exchange: permite enrutar cada respuesta a una routing key específica (answer.<user_id>).
    canal.exchange_declare(exchange=TOPIC_EXCHANGE, exchange_type="topic", durable=True)
    # durable=True: la cola sobrevive si RabbitMQ se reinicia, sin perder mensajes pendientes.
    canal.queue_declare(queue=WORK_QUEUE, durable=True)
    # prefetch_count=1: el dispatcher solo toma un mensaje a la vez antes de confirmar (ack).
    # Esto implementa balanceo justo si hubiera múltiples instancias del dispatcher.
    canal.basic_qos(prefetch_count=1)

    def procesar_consulta(ch, method, properties, body):
        consulta = json.loads(body)
        user_id  = consulta.get("user_id", "desconocido")
        trayecto = consulta.get("trayecto", [])
        print(f"[DISPATCHER] Consulta de {user_id}: {trayecto}")

        # Construye la respuesta consultando el estado local; no necesita al processor.
        respuesta = {
            zona: zone_states_local[zona]["estado"] if zona in zone_states_local else "SIN_DATOS"
            for zona in trayecto
        }

        # La routing key "answer.<user_id>" garantiza que solo ese cliente reciba la respuesta.
        routing_key = f"answer.{user_id}"
        canal.basic_publish(
            exchange=TOPIC_EXCHANGE,
            routing_key=routing_key,
            body=json.dumps({"user_id": user_id, "trayecto": respuesta})
        )
        print(f"[DISPATCHER] Respuesta enviada a '{routing_key}' → {respuesta}")

        # basic_ack le dice a RabbitMQ que el mensaje fue procesado y puede eliminarse de la cola.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"[DISPATCHER] Escuchando Work Queue '{WORK_QUEUE}'...")
    canal.basic_consume(queue=WORK_QUEUE, on_message_callback=procesar_consulta)
    canal.start_consuming()

def main():
    # Se necesitan DOS conexiones independientes porque pika.BlockingConnection
    # no es thread-safe: usar la misma desde dos hilos provoca errores de frame.
    conexion1 = conectar_rabbitmq()
    conexion2 = conectar_rabbitmq()

    threading.Thread(target=hilo_fanout,      args=(conexion1,), daemon=True).start()
    threading.Thread(target=hilo_work_queue,  args=(conexion2,), daemon=True).start()

    print("[DISPATCHER] Ambos hilos activos. Esperando mensajes...")
    while True:
        time.sleep(1)  # mantiene el proceso principal vivo sin consumir CPU

if __name__ == "__main__":
    main()
