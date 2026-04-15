#!/usr/bin/env python3
"""
test_template.py — Paso 1 del diagnóstico de QR en correos.

Qué hace:
  1. Renderiza purchase_confirmation.html con 3 tickets ficticios (mismos
     códigos que reportó el usuario: CINE-01FMBB3, CINE-9WBNKSS, CINE-57RQL2U)
  2. Guarda el resultado como test_email_output.html
  3. Lo abre en el navegador predeterminado para ver si los 3 QRs aparecen
  4. (Opcional) Lo envía por SMTP al correo indicado en --to

Uso:
  # Solo browser
  python test_template.py

  # Browser + enviar correo de prueba
  python test_template.py --to andres.vorja.vorja@gmail.com
"""

import argparse
import os
import sys
import webbrowser
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import aiosmtplib
import asyncio
from jinja2 import Environment, FileSystemLoader, select_autoescape

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
TEMPLATE_DIR = BASE_DIR / "app" / "templates"
OUTPUT_FILE  = BASE_DIR / "test_email_output.html"

# ── Mock data — exactamente los 3 tickets del usuario ─────────────────────────
MOCK_TICKETS = [
    {"code": "CINE-01FMBB3", "seat": "GENERAL-1", "status": "ACTIVE"},
    {"code": "CINE-9WBNKSS", "seat": "GENERAL-2", "status": "ACTIVE"},
    {"code": "CINE-57RQL2U", "seat": "GENERAL-3", "status": "ACTIVE"},
]

MOCK_DATA = {
    "customer_name":    "Andrés (Test)",
    "movie_title":      "Película de Prueba",
    "movie_genre":      "Acción",
    "movie_duration":   120,
    "movie_rating":     "PG-13",
    "theater_name":     "Cine Colombia Centro",
    "theater_location": "Bogotá, Colombia",
    "show_date":        "25/12/2025",
    "show_time":        "20:00",
    "show_format":      "2D",
    "purchase_id":      999,
    "purchase_date":    "14/04/2026",
    "purchase_time":    "10:30",
    "quantity":         3,
    "total_amount":     "$45,000 COP",
    "status":           "CONFIRMADO",
    "tickets":          MOCK_TICKETS,
    "payment_last_four": "1234",
    "transaction_id":   "TXN-TEST-123456",
    "support_email":    "support@cinema.com",
}

# ── Render template ────────────────────────────────────────────────────────────
def render_template() -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("purchase_confirmation.html")
    return template.render(**MOCK_DATA)


# ── Send via SMTP ──────────────────────────────────────────────────────────────
async def send_test_email(html: str, to_email: str) -> None:
    email_user     = os.getenv("EMAIL_USER")
    email_password = os.getenv("EMAIL_APP_PASSWORD")
    email_port     = int(os.getenv("EMAIL_PORT", "465"))
    email_host     = os.getenv("EMAIL_HOST", "smtp.gmail.com")
    if not email_user or not email_password:
        raise EnvironmentError("EMAIL_USER y EMAIL_APP_PASSWORD deben estar definidos en .env")

    msg = MIMEMultipart("alternative")
    msg["From"]    = f'"Cinema Test" <{email_user}>'
    msg["To"]      = to_email
    msg["Subject"] = f"[TEST] Confirmación 3 tickets — {datetime.now().strftime('%H:%M:%S')}"
    msg.attach(MIMEText(html, "html", "utf-8"))

    print(f"\n Enviando correo de prueba a {to_email} ...")
    try:
        async with aiosmtplib.SMTP(
            hostname=email_host,
            port=email_port,
            use_tls=(email_port == 465),
        ) as smtp:
            await smtp.login(email_user, email_password)
            await smtp.send_message(msg)
        print("    Correo enviado. Revisa tu bandeja de entrada.")
    except Exception as e:
        print(f"    Error enviando correo: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnóstico de template de email con 3 QR")
    parser.add_argument("--to", metavar="EMAIL", help="Enviar copia del HTML por SMTP a este correo")
    args = parser.parse_args()

    print(" Renderizando template con 3 tickets de prueba...")
    print(f"   Tickets: {[t['code'] for t in MOCK_TICKETS]}")

    html = render_template()
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"    HTML guardado en: {OUTPUT_FILE}")

    # Verificar cuántas veces aparece cada código en el HTML
    for ticket in MOCK_TICKETS:
        code = ticket["code"]
        count = html.count(code)
        print(f"   🔍 '{code}' aparece {count} veces en el HTML (esperable: 2 — en texto y en URL del QR)")

    # Verificar cuántos img src de QR hay
    qr_url_count = html.count("api.qrserver.com")
    print(f"     URLs de QR encontradas: {qr_url_count} (esperadas: {len(MOCK_TICKETS)})")

    print("\n Abriendo en el navegador...")
    webbrowser.open(f"file://{OUTPUT_FILE.resolve()}")

    if args.to:
        asyncio.run(send_test_email(html, args.to))

    print("\n Resumen del diagnóstico:")
    print(f"   - HTML tiene {qr_url_count} QRs generados")
    print(f"   - Si el navegador muestra {len(MOCK_TICKETS)} QRs → el template está OK, el problema es de transporte/cliente de correo")
    print(f"   - Si el navegador solo muestra 1 QR → el problema es del template HTML o CSS")
    if args.to:
        print(f"   - Si el correo recibido muestra {len(MOCK_TICKETS)} QRs → problema resuelto")
        print(f"   - Si el correo recibido muestra solo 1 QR → problema del cliente de correo (Gmail)")


if __name__ == "__main__":
    main()
