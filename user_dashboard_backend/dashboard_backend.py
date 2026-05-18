# Microservicio 4 — Puente RabbitMQ → WebSocket: recibe el estado del Fanout
# y lo retransmite en tiempo real a todos los clientes del dashboard.html.

import json
import time
import os
import asyncio
import threading
import pika
import websockets

RABBITMQ_HOST   = os.environ.get("RABBITMQ_HOST", "localhost")
FANOUT_EXCHANGE = "traffic_updates"
WS_HOST         = "0.0.0.0"
WS_PORT         = 8765

clientes_ws  = set()   # clientes WebSocket actualmente conectados
ultimo_estado = {}     # último snapshot recibido; se envía a quienes se conecten tarde
main_loop     = None   # referencia al event loop de asyncio (necesaria para cruzar hilos)

async def handler_websocket(websocket):
    # Se invoca por cada nueva conexión al WebSocket.
    print(f"[DASHBOARD] Cliente conectado: {websocket.remote_address}")
    clientes_ws.add(websocket)

    # Si ya hay estado disponible, se lo enviamos inmediatamente al recién conectado.
    if ultimo_estado:
        await websocket.send(json.dumps(ultimo_estado))

    try:
        # Iteramos sobre los mensajes entrantes solo para mantener la conexión viva;
        # el dashboard no envía datos al servidor, así que los descartamos con _.
        async for _ in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clientes_ws.discard(websocket)
        print("[DASHBOARD] Cliente desconectado")

async def broadcast_a_clientes(estado):
    # Envía el mismo mensaje a todos los clientes en paralelo con gather().
    # return_exceptions=True evita que un cliente desconectado cancele el resto.
    if clientes_ws:
        await asyncio.gather(
            *[c.send(json.dumps(estado)) for c in clientes_ws],
            return_exceptions=True
        )

def callback_rabbitmq(ch, method, properties, body):
    # Este callback corre en el hilo de pika, no en el loop asyncio.
    # run_coroutine_threadsafe() es la única forma segura de programar una corrutina
    # desde un hilo externo sobre el loop principal ya en ejecución.
    global ultimo_estado
    ultimo_estado = json.loads(body)
    print(f"[DASHBOARD] Estado recibido → {list(ultimo_estado.keys())}")
    if main_loop:
        asyncio.run_coroutine_threadsafe(broadcast_a_clientes(ultimo_estado), main_loop)

def hilo_rabbitmq():
    # Corre en su propio hilo porque pika.start_consuming() es bloqueante.
    # Si la conexión cae, el bucle while reintenta automáticamente.
    while True:
        try:
            conexion = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            canal    = conexion.channel()
            canal.exchange_declare(exchange=FANOUT_EXCHANGE, exchange_type="fanout", durable=True)

            # Cola anónima y exclusiva: RabbitMQ la nombra y la borra al desconectarse.
            # exclusive=True garantiza que solo este servicio la usa.
            nombre_cola = canal.queue_declare(queue="", exclusive=True).method.queue
            canal.queue_bind(exchange=FANOUT_EXCHANGE, queue=nombre_cola)

            print(f"[DASHBOARD] Escuchando Fanout '{FANOUT_EXCHANGE}'...")
            canal.basic_consume(queue=nombre_cola, on_message_callback=callback_rabbitmq, auto_ack=True)
            canal.start_consuming()
        except Exception as e:
            print(f"[DASHBOARD] Error RabbitMQ: {e}. Reintentando en 5s...")
            time.sleep(5)

async def main():
    global main_loop
    # Capturamos el loop antes de lanzar el hilo para que callback_rabbitmq pueda usarlo.
    main_loop = asyncio.get_event_loop()

    threading.Thread(target=hilo_rabbitmq, daemon=True).start()

    print(f"[DASHBOARD] WebSocket en ws://{WS_HOST}:{WS_PORT}")
    async with websockets.serve(handler_websocket, WS_HOST, WS_PORT):
        await asyncio.Future()  # bloquea indefinidamente sin consumir CPU

if __name__ == "__main__":
    asyncio.run(main())
