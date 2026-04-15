# app/services/email_service.py
#
# Versión desacoplada del email_service del monolito.
# Trabaja con dicts planos del evento Kafka — sin modelos SQLAlchemy ni acceso a BD.
import base64
import io
import logging
import os
from datetime import date, datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib
import qrcode
from jinja2 import Environment, FileSystemLoader, select_autoescape
from zoneinfo import ZoneInfo

from app.core.config import settings

_BOGOTA_TZ = ZoneInfo("America/Bogota")

logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")
_template_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)


def _generate_qr_base64(data: str) -> str:
    """Generates a QR code PNG and returns it as a base64 data URI for inline email embedding."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=6,
        border=2,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def _format_currency(amount: float) -> str:
    return f"${amount:,.0f} COP"


def _format_datetime(iso_str: str) -> tuple[str, str]:
    """Returns (date_str, time_str) in Bogotá timezone from an ISO datetime string."""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_bogota = dt.astimezone(_BOGOTA_TZ)
        return dt_bogota.strftime("%d/%m/%Y"), dt_bogota.strftime("%H:%M")
    except Exception:
        return "N/A", "N/A"


def _format_show_date(iso_date: str | None) -> str:
    """Returns a human-readable show date from a YYYY-MM-DD string."""
    if not iso_date:
        return "Por confirmar"
    try:
        d = date.fromisoformat(iso_date)
        return d.strftime("%d/%m/%Y")
    except Exception:
        return iso_date


async def _send_raw_email(subject: str, html_content: str, to_email: str) -> bool:
    """Low-level SMTP send. Handles both port 465 (SSL) and 587 (STARTTLS)."""
    try:
        if not settings.EMAIL_USER or not settings.EMAIL_APP_PASSWORD:
            logger.info("SIMULATED EMAIL → To: %s | Subject: %s", to_email, subject)
            return True

        msg = MIMEMultipart("alternative")
        msg["From"] = f'"Cinema Tickets" <{settings.EMAIL_USER}>'
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        if settings.EMAIL_PORT == 465:
            async with aiosmtplib.SMTP(
                hostname=settings.EMAIL_HOST,
                port=settings.EMAIL_PORT,
                use_tls=True,
            ) as smtp:
                await smtp.login(settings.EMAIL_USER, settings.EMAIL_APP_PASSWORD)
                await smtp.send_message(msg)
        else:
            async with aiosmtplib.SMTP(
                hostname=settings.EMAIL_HOST,
                port=settings.EMAIL_PORT,
            ) as smtp:
                await smtp.starttls()
                await smtp.login(settings.EMAIL_USER, settings.EMAIL_APP_PASSWORD)
                await smtp.send_message(msg)

        logger.info("Email sent → %s", to_email)
        return True

    except Exception as e:
        logger.error("Error sending email to %s: %s", to_email, e)
        return False


async def send_refund_confirmation(event: dict) -> bool:
    """
    Envía email de confirmación de reembolso desde un evento order.refunded.

    Expected event keys:
        order_id, user_email, customer_name (optional),
        movie_title (optional), quantity, total_amount,
        transaction_id, refund_id (optional)
    """
    try:
        cancel_date, _ = _format_datetime(event.get("cancelled_at", ""))
        if cancel_date == "N/A":
            from datetime import datetime
            cancel_date = datetime.now().strftime("%d/%m/%Y")

        template_data = {
            "customer_name":  event.get("customer_name", "Cliente"),
            "order_id":       event.get("order_id", "?"),
            "movie_title":    event.get("movie_title", "N/A"),
            "quantity":       event.get("quantity", 0),
            "total_amount":   _format_currency(event.get("total_amount", 0)),
            "transaction_id": event.get("transaction_id", "N/A"),
            "refund_id":      event.get("refund_id", "N/A"),
            "cancel_date":    cancel_date,
            "support_email":  settings.SUPPORT_EMAIL,
        }

        template = _template_env.get_template("refund_confirmation.html")
        html = template.render(**template_data)

        subject = f"Reembolso Procesado #{event.get('order_id')} - Cinema Tickets"
        return await _send_raw_email(subject, html, event["user_email"])

    except Exception as e:
        logger.error("Error building refund confirmation email: %s", e)
        return False


async def send_password_reset_email(event: dict) -> bool:
    """
    Send a password reset email triggered by repeated failed login attempts.

    Expected event keys:
        user_email, customer_name, reset_token
    """
    try:
        reset_link = f"{settings.FRONTEND_URL}/reset-password?token={event['reset_token']}"

        template_data = {
            "customer_name": event.get("customer_name", "Cliente"),
            "reset_link":    reset_link,
            "support_email": settings.SUPPORT_EMAIL,
        }

        template = _template_env.get_template("password_reset.html")
        html = template.render(**template_data)

        subject = "Restablece tu contraseña - Cinema Tickets"
        return await _send_raw_email(subject, html, event["user_email"])

    except Exception as e:
        logger.error("Error building password reset email: %s", e)
        return False


async def send_purchase_confirmation(event: dict) -> bool:
    """
    Send purchase confirmation email from a payment.success Kafka event dict.

    Expected event keys from the active microservice flow:
        order_id, user_email, customer_name,
        movie_title, movie_genre, movie_duration, movie_rating,
        quantity, total_amount, transaction_id, payment_last_four,
        purchase_created_at, tickets [{code, seat, status}]
    """
    try:
        purchase_date, purchase_time = _format_datetime(
            event.get("purchase_created_at", "")
        )
        tickets = event.get("tickets", [])

        logger.info(
            "Building purchase confirmation email | order_id=%s | tickets_count=%d | tickets=%s",
            event.get("order_id"),
            len(tickets),
            [t.get("code") for t in tickets],
        )

        # Generate inline QR codes (base64 PNG) for each ticket so they render
        # in every email client without depending on an external image API.
        tickets_with_qr = []
        for t in tickets:
            qr_data = f"{t.get('code')}|{t.get('seat', '')}"
            tickets_with_qr.append({
                **t,
                "qr_image": _generate_qr_base64(qr_data),
            })

        template_data = {
            "customer_name": event.get("customer_name", "Cliente"),
            "movie_title": event.get("movie_title", "N/A"),
            "movie_genre": event.get("movie_genre", "N/A"),
            "movie_duration": event.get("movie_duration", "N/A"),
            "movie_rating": event.get("movie_rating", "N/A"),
            # Showtime fields — come from booking-service event_context
            "theater_name": event.get("theater_name") or "Por confirmar",
            "theater_location": event.get("theater_location") or "Por confirmar",
            "show_date": _format_show_date(event.get("show_date")),
            "show_time": event.get("show_time") or "Por confirmar",
            "show_format": event.get("show_format") or "2D",
            # Purchase details
            "purchase_id": event.get("order_id"),
            "purchase_date": purchase_date,
            "purchase_time": purchase_time,
            "quantity": event.get("quantity"),
            "total_amount": _format_currency(event.get("total_amount", 0)),
            "status": "CONFIRMADO",
            "tickets": tickets_with_qr,
            "payment_last_four": event.get("payment_last_four", "****"),
            "transaction_id": event.get("transaction_id", "N/A"),
            "support_email": settings.SUPPORT_EMAIL,
        }

        template = _template_env.get_template("purchase_confirmation.html")
        html = template.render(**template_data)

        subject = f"Confirmación de Compra #{event.get('order_id')} - Cinema Tickets"
        return await _send_raw_email(subject, html, event["user_email"])

    except Exception as e:
        logger.error("Error building purchase confirmation email: %s", e)
        return False
