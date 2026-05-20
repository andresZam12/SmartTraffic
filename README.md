# 🚦 SmartTraffic — Sistema de Monitoreo de Tráfico Vehicular Inteligente

Proyecto Final — Sistemas Operativos y Sistemas Distribuidos  
Stack: Python · Apache Kafka · RabbitMQ · Docker Compose · WebSocket

---

## Arquitectura

```
traffic_sensor_simulator
        │  (Kafka: raw_traffic_data)
        ▼
traffic_density_processor ──────────────────────────────────────────────┐
        │  (RabbitMQ Fanout: traffic_updates)                            │
        ├──────────────────────────────────┐                             │
        ▼                                  ▼                             │
user_dashboard_backend            alert_dispatcher ◄── Work Queue ◄── query_client
        │  (WebSocket)                     │  (Topic Exchange)               ▲
        ▼                                  └─────────────────────────────────┘
  dashboard.html                                     respuesta asíncrona

traffic_archiver ◄── (Kafka: raw_traffic_data, grupo independiente)
```

## Microservicios

| Servicio | Rol | Entrada | Salida |
|---|---|---|---|
| `traffic_sensor_simulator` | Productor Kafka | — | Kafka `raw_traffic_data` |
| `traffic_density_processor` | Consumer Kafka + Producer RabbitMQ | Kafka | RabbitMQ Fanout |
| `traffic_archiver` | Consumer Kafka | Kafka | Consola (simula BD) |
| `user_dashboard_backend` | Consumer RabbitMQ + WebSocket | RabbitMQ Fanout | WebSocket |
| `alert_dispatcher` | Consumer/Producer RabbitMQ | Fanout + Work Queue | Topic Exchange |
| `query_client` | Producer/Consumer RabbitMQ | — | Work Queue → espera Topic |

---

## Cómo correr el proyecto

### Requisitos
- Docker Desktop instalado y corriendo
- Git (opcional)

### Pasos

```bash
# 1. Clonar o descargar el proyecto
cd smarttraffic

# 2. Construir las imágenes y levantar los 9 contenedores
docker compose up -d --build

# 3. Verificar que todos estén corriendo
docker compose ps

# 4. Abrir el dashboard en el navegador
#    Doble clic en: dashboard.html
#    Muestra las 5 zonas en tiempo real con sparklines y contador de actualización.

# 5. Inspeccionar datos como JSON (API REST)
#    http://localhost:8766/estado    → estado actual de todas las zonas
#    http://localhost:8766/sensores  → últimas 20 capturas con timestamp
#    http://localhost:8766/health    → verificación del servicio

# 6. Panel de administración de RabbitMQ
#    http://localhost:15672  →  usuario: guest  /  contraseña: guest
#    Aquí puedes ver exchanges, colas y tasas de mensajes en tiempo real.
```

### Ver logs por servicio

```bash
# Ver todos los logs en tiempo real
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f density_processor
docker-compose logs -f alert_dispatcher
docker-compose logs -f query_client
```

### Detener el sistema

```bash
docker-compose down
```

---

## Patrones de RabbitMQ implementados

### 1. Fanout Exchange (`traffic_updates`)
- El `density_processor` publica el estado cada 5s
- **TODOS** los servicios suscritos reciben el mismo mensaje
- Lo usan: `user_dashboard_backend` y `alert_dispatcher`

### 2. Work Queue (`query_traffic_queue`)
- `query_client` publica preguntas
- `alert_dispatcher` las consume de a una (prefetch=1)
- Si hubiera múltiples dispatchers, el trabajo se balancearía

### 3. Topic Exchange (`query_answers`)
- `alert_dispatcher` publica respuestas con routing key `answer.<user_id>`
- Cada `query_client` solo recibe SUS respuestas gracias a la routing key única

---

## Justificación de diseño: ¿Por qué Kafka y no solo RabbitMQ?

**Kafka se usa para:**
- Flujo de datos de sensores (alto volumen, orden garantizado por partición)
- Múltiples consumidores independientes leyendo el mismo topic (`processor` + `archiver`)
- Procesamiento stateful: el `processor` mantiene estado agregado leyendo el stream

**RabbitMQ se usa para:**
- Distribución de estado a múltiples servicios (Fanout)
- Mensajería de consultas asíncronas (Work Queue + Topic)
- Comunicación entre servicios de negocio (no de datos brutos)

**La diferencia clave:** Kafka es un log de eventos inmutable y replayable.
RabbitMQ es un bus de mensajes donde el mensaje desaparece tras ser consumido.

---

## Flujo de datos completo

```
1. sensor.py genera {"zone_id": "Zona C", "vehicle_count": 85}
2. processor.py lo consume de Kafka y actualiza zone_states["Zona C"] = "CONGESTIONADA"
3. Cada 5s, processor.py publica el Map completo en RabbitMQ Fanout
4. dashboard_backend.py recibe el Map → lo envía por WebSocket → el HTML se actualiza
5. alert_dispatcher.py recibe el Map → actualiza su copia local → genera alerta si cambió
6. query_client.py envía {"user_id": "client_abc123", "trayecto": ["Zona A", "Zona C"]}
7. alert_dispatcher.py consulta su Map local → responde en Topic Exchange "answer.client_abc123"
8. query_client.py imprime la respuesta asíncrona
```
