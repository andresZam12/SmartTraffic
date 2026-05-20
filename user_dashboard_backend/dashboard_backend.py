# Microservicio 4 — Puente RabbitMQ → WebSocket + API REST
# Recibe el estado del Fanout y lo retransmite al dashboard via WebSocket.
# Expone también GET /estado y GET /sensores como endpoints HTTP para inspección.

import json
import time
import os
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pika
import websockets

RABBITMQ_HOST   = os.environ.get("RABBITMQ_HOST", "localhost")
FANOUT_EXCHANGE = "traffic_updates"
WS_HOST         = "0.0.0.0"
WS_PORT         = 8765
HTTP_PORT       = 8766   # puerto para la API REST de inspección

clientes_ws   = set()
ultimo_estado = {}
historial_sensores = []   # guarda los últimos 20 eventos recibidos del Fanout
main_loop     = None

# ── API REST simple (sin frameworks externos) ──────────────────────────────

class APIHandler(BaseHTTPRequestHandler):
    # Silencia los logs de acceso HTTP para no saturar la consola.
    def log_message(self, format, *args): pass

    def _responder(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/estado":
            # Devuelve el estado actual de todas las zonas.
            self._responder(ultimo_estado if ultimo_estado else {"mensaje": "Sin datos aún"})

        elif self.path == "/sensores":
            # Devuelve los últimos 20 snapshots recibidos del procesador.
            self._responder(historial_sensores if historial_sensores else [])

        elif self.path == "/health":
            # Endpoint de salud para verificar que el servicio está vivo.
            self._responder({"status": "ok", "zonas": len(ultimo_estado)})

        else:
            self._responder({"error": "Ruta no encontrada. Usa /estado, /sensores o /health"}, 404)

def hilo_http():
    # Corre el servidor HTTP en un hilo separado para no bloquear asyncio.
    servidor = HTTPServer(("0.0.0.0", HTTP_PORT), APIHandler)
    print(f"[DASHBOARD] API REST en http://0.0.0.0:{HTTP_PORT}")
    servidor.serve_forever()

# ── WebSocket ──────────────────────────────────────────────────────────────

async def handler_websocket(websocket):
    print(f"[DASHBOARD] Cliente WebSocket conectado: {websocket.remote_address}")
    clientes_ws.add(websocket)

    if ultimo_estado:
        await websocket.send(json.dumps(ultimo_estado))

    try:
        async for _ in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clientes_ws.discard(websocket)
        print("[DASHBOARD] Cliente WebSocket desconectado")

async def broadcast_a_clientes(estado):
    if clientes_ws:
        await asyncio.gather(
            *[c.send(json.dumps(estado)) for c in clientes_ws],
            return_exceptions=True
        )

# ── RabbitMQ ───────────────────────────────────────────────────────────────

def callback_rabbitmq(ch, method, properties, body):
    global ultimo_estado
    estado = json.loads(body)
    ultimo_estado = estado

    # Guarda el snapshot en el historial (máximo 20 entradas).
    historial_sensores.append({"timestamp": time.strftime("%H:%M:%S"), "zonas": estado})
    if len(historial_sensores) > 20:
        historial_sensores.pop(0)

    print(f"[DASHBOARD] Estado recibido → {list(estado.keys())}")
    if main_loop:
        asyncio.run_coroutine_threadsafe(broadcast_a_clientes(estado), main_loop)

def hilo_rabbitmq():
    while True:
        try:
            conexion = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            canal    = conexion.channel()
            canal.exchange_declare(exchange=FANOUT_EXCHANGE, exchange_type="fanout", durable=True)
            nombre_cola = canal.queue_declare(queue="", exclusive=True).method.queue
            canal.queue_bind(exchange=FANOUT_EXCHANGE, queue=nombre_cola)
            print(f"[DASHBOARD] Escuchando Fanout '{FANOUT_EXCHANGE}'...")
            canal.basic_consume(queue=nombre_cola, on_message_callback=callback_rabbitmq, auto_ack=True)
            canal.start_consuming()
        except Exception as e:
            print(f"[DASHBOARD] Error RabbitMQ: {e}. Reintentando en 5s...")
            time.sleep(5)

# ── Main ───────────────────────────────────────────────────────────────────

async def main():
    global main_loop
    main_loop = asyncio.get_event_loop()

    threading.Thread(target=hilo_rabbitmq, daemon=True).start()
    threading.Thread(target=hilo_http,     daemon=True).start()

    print(f"[DASHBOARD] WebSocket en ws://{WS_HOST}:{WS_PORT}")
    async with websockets.serve(handler_websocket, WS_HOST, WS_PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
