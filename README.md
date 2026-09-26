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

## Flujo de trabajo: Gitflow

| Rama        | Sale de   | Entra a (vía PR)         | Método en GitHub | Para |
| ----------- | --------- | ------------------------ | ---------------- | ---- |
| `main`      | —         | —                        | —                | Lo que está en producción. Cada merge es una versión. |
| `develop`   | `main`    | —                        | —                | Integración de lo próximo a publicar. Rama por defecto. |
| `feature/*` | `develop` | `develop`                | **Squash**       | Una funcionalidad o cambio: `feature/mi-cambio`. |
| `release/*` | `develop` | `main` y luego `develop` | **Merge** a `main`; **Squash** a `develop` | Preparar una versión: `release/1.0.0`. Solo ajustes finales. |
| `hotfix/*`  | `main`    | `main` y luego `develop` | **Merge** a `main`; **Squash** a `develop` | Corrección urgente en producción. |

- **Nadie hace push directo** a `main` ni a `develop`: todo entra por pull request, con los checks de CI en verde.
- **En `develop` se usa squash:** cada feature queda como un solo commit con el título del PR.
- **En `main` se usa merge commit:** cada release o hotfix queda visible como una unidad.
- **Todavía no hay releases:** la app no está completa, así que `main` se queda como está hasta el
  primer `release/*`. Desde entonces, cada versión se etiqueta en `main` (`git tag -a v1.0.0`) con
  [versionado semántico](https://semver.org/lang/es/).

```bash
git switch develop && git pull
git switch -c feature/mi-cambio
# ...commits...
git push -u origin feature/mi-cambio   # abrir PR hacia develop → Squash and merge
```

## CI/CD

GitHub Actions (`.github/workflows/`) corre en cada PR hacia `main` o `develop`. Los rulesets exigen
estos checks; si se renombra un job, hay que actualizar `.github/rulesets/*.json`.

| Check | Qué revisa |
| ----- | ---------- |
| `Lint` | Ruff con las reglas de `ruff.toml`. |
| `Calidad y build` | Instala las dependencias, compila todo el código y carga la app con configuración falsa (sin base de datos ni Kafka). |
| `Imagen Docker` | Construye la imagen y comprueba que la app carga dentro de ella, sin red. |

Con cada push a `develop` o `main` (es decir, al fusionar un PR), y solo si pasaron los checks, se
publica en Docker Hub **la misma imagen que se probó** (no se reconstruye):

- `develop` → `<usuario>/notification-service-cinema:develop` y `:<sha>`
- `main` → `<usuario>/notification-service-cinema:latest` y `:<sha>`

El flujo no despliega en ningún servicio (tampoco en Render): solo publica la imagen.

### Configuración en GitHub (una vez)

- **Rulesets:** `main` y `develop` se protegen importando `.github/rulesets/main.json` y
  `.github/rulesets/develop.json` en *Settings → Rules → Rulesets → Import a ruleset*. Exigen PR, los
  checks de la tabla de arriba, y no permiten borrar la rama ni forzar pushes. `main` solo acepta
  merge commit y `develop` solo squash.
- **Settings → General:** rama por defecto `develop`; permitir merge commits y squash (no rebase);
  activar *Automatically delete head branches*.
- **Secrets** (*Settings → Secrets and variables → Actions*): `DOCKER_USERNAME` y `DOCKER_TOKEN`
  (token de acceso de Docker Hub con permiso de escritura).
