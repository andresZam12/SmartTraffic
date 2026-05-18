# Microservicio 6 — Cliente asíncrono: simula un usuario que pregunta por el estado de un trayecto.
# Implementa el patrón Request/Reply asíncrono:
#   - Publica la pregunta en una Work Queue (no espera bloqueado).
#   - Escucha la respuesta en su propia cola del Topic Exchange con routing key única.

import json
import time
import os
import uuid
import threading
import random
import pika

RABBITMQ_HOST  = os.environ.get("RABBITMQ_HOST", "localhost")
WORK_QUEUE     = "query_traffic_queue"
TOPIC_EXCHANGE = "query_answers"
CONSULTAR_CADA = 15
ZONAS          = ["Zona A", "Zona B", "Zona C", "Zona D", "Zona E"]

# ID único generado al arrancar: identifica a este cliente entre todos los que puedan correr.
USER_ID = f"client_{uuid.uuid4().hex[:6]}"

def conectar_rabbitmq():
    while True:
        try:
            conexion = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print(f"[CLIENT-{USER_ID}] Conectado a RabbitMQ")
            return conexion
        except Exception:
            print(f"[CLIENT-{USER_ID}] RabbitMQ no disponible, reintentando en 5s...")
            time.sleep(5)

def hilo_escuchar_respuestas(conexion):
    # Hilo siempre activo que espera la respuesta del dispatcher.
    canal = conexion.channel()
    canal.exchange_declare(exchange=TOPIC_EXCHANGE, exchange_type="topic", durable=True)

    # Cola exclusiva y anónima: solo este cliente la usa y se elimina al desconectarse.
    nombre_cola = canal.queue_declare(queue="", exclusive=True).method.queue

    # La routing key "answer.<USER_ID>" es el "buzón" de este cliente.
    # El dispatcher publica en esa key exacta para que solo aquí llegue la respuesta.
    routing_key = f"answer.{USER_ID}"
    canal.queue_bind(exchange=TOPIC_EXCHANGE, queue=nombre_cola, routing_key=routing_key)

    def recibir_respuesta(ch, method, properties, body):
        trayecto = json.loads(body).get("trayecto", {})
        print(f"\n[CLIENT-{USER_ID}] RESPUESTA RECIBIDA:")
        for zona, estado in trayecto.items():
            icono = {"DESPEJADA": "🟢", "MODERADA": "🟡", "CONGESTIONADA": "🔴"}.get(estado, "⚪")
            print(f"   {icono}  {zona}: {estado}")
        print()

    print(f"[CLIENT-{USER_ID}] Escuchando respuestas en '{routing_key}'...")
    canal.basic_consume(queue=nombre_cola, on_message_callback=recibir_respuesta, auto_ack=True)
    canal.start_consuming()

def hilo_enviar_consultas(conexion):
    # Hilo que cada CONSULTAR_CADA segundos envía una nueva pregunta al dispatcher.
    canal = conexion.channel()
    canal.queue_declare(queue=WORK_QUEUE, durable=True)

    time.sleep(10)  # espera inicial para que el sistema esté listo antes de la primera consulta

    while True:
        trayecto = random.sample(ZONAS, random.randint(2, 3))
        consulta = {"user_id": USER_ID, "trayecto": trayecto}

        # exchange="" significa publicar directamente en la cola por nombre (sin exchange intermedio).
        # delivery_mode=2 marca el mensaje como persistente en disco.
        canal.basic_publish(
            exchange="",
            routing_key=WORK_QUEUE,
            body=json.dumps(consulta),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[CLIENT-{USER_ID}] Consulta enviada: {trayecto}")
        time.sleep(CONSULTAR_CADA)

def main():
    print(f"[CLIENT-{USER_ID}] Iniciando...")

    # Dos conexiones separadas porque pika no es thread-safe con conexión compartida entre hilos.
    conexion1 = conectar_rabbitmq()
    conexion2 = conectar_rabbitmq()

    threading.Thread(target=hilo_escuchar_respuestas, args=(conexion1,), daemon=True).start()
    threading.Thread(target=hilo_enviar_consultas,    args=(conexion2,), daemon=True).start()

    print(f"[CLIENT-{USER_ID}] Activo. Consultas cada {CONSULTAR_CADA}s.")
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
