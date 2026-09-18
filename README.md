# notification-service-cinema

Envía los emails transaccionales del sistema (confirmación de compra,
reembolso, reseteo de contraseña) reaccionando a eventos de Kafka — no
expone lógica de negocio propia ni tiene base de datos.

## Responsabilidad

Puramente reactivo: consume eventos, renderiza una plantilla HTML y envía un
correo. No valida reglas de negocio ni persiste estado — si el email falla,
se loguea el error y se sigue (ver "Manejo de errores").

## Stack

- FastAPI + Uvicorn, puerto **8001** (sin ruta pública vía Traefik — servicio interno)
- `aiokafka` para el consumer
- `aiosmtplib` + `jinja2` para el envío (plantillas HTML en `app/templates/`)
- Sin base de datos propia (no usa Alembic ni psycopg2 pese a estar en `requirements.txt`)

## Eventos Kafka

Consume (grupo `notification-service-group`):

| Topic | Envía |
|---|---|
| `purchase.confirmed` | Email de confirmación de compra |
| `order.refunded` | Email de confirmación de reembolso |
| `auth.password_reset_requested` | Email de reseteo de contraseña |

**Por qué `purchase.confirmed` y no `payment.success`:** `booking-service`
publica `purchase.confirmed` solo *después* de confirmar la compra sin
conflictos (tickets ya materializados en BD). Escuchar `payment.success`
directamente mandaría el correo antes de saber si la compra sobrevive a un
posible conflicto de doble venta.

Contrato completo de cada evento (payload, productor, semántica): ver
`../kafka-schemas-cinema/event_contracts_operativos.md`.

`auto_offset_reset="latest"` — al arrancar sin offsets previos no reenvía
eventos históricos. El commit del offset es manual y ocurre siempre después
de procesar el evento, incluso si el envío falla (ver abajo), para no
reintentar indefinidamente un mensaje "envenenado".

## Manejo de errores

Si el envío de un email falla (SMTP caído, credenciales inválidas), se
loguea como error pero el offset se comitea igual — **no hay reintento**. Es
una decisión deliberada de este servicio: un correo transaccional perdido no
debe bloquear la cola para los demás eventos.

Si `EMAIL_USER` o `EMAIL_APP_PASSWORD` están vacíos, no se intenta conectar
a SMTP: se loguea `SIMULATED EMAIL → To: ... | Subject: ...` y se considera
enviado. Útil para desarrollo local sin credenciales de Gmail reales.

## Variables de entorno

| Variable | Para qué |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_API_KEY` / `KAFKA_API_SECRET` | Credenciales de Confluent Cloud |
| `KAFKA_GROUP_ID` | Grupo de consumidor (default `notification-service-group`) |
| `EMAIL_HOST` / `EMAIL_PORT` | Servidor SMTP (default Gmail, 465 SSL o 587 STARTTLS) |
| `EMAIL_USER` / `EMAIL_APP_PASSWORD` | Credenciales SMTP — vacías = modo simulado |
| `SUPPORT_EMAIL` | Email de soporte inyectado en las plantillas |
| `FRONTEND_URL` | Base para el link de reseteo de contraseña |

Ver `.env.example` para la plantilla completa. `Settings.model_config` tiene
`extra: "ignore"` — no falla si Render inyecta variables no declaradas
(ver `../IMPLEMENTATION-GUIDE.md` Fase 0 para el porqué de esto).

## Dependencias de red

Ninguna. No llama por HTTP a otro microservicio — solo consume Kafka y habla
SMTP hacia afuera.

## Correr en local

```bash
# Standalone (requiere Kafka accesible y las credenciales en .env)
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001

# Como parte del stack completo
cd ../infra-cinema
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build notification-service
```

`GET /health` reporta `degraded` si la tarea del consumer no está corriendo.
