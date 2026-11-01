"""Shared alerting helpers for Airflow DAGs (email version).

Lives in airflow/plugins/ so every DAG can do:
    from alerting import notify_failure, ping_heartbeat

Setup in the Airflow UI:
  - Admin -> Connections: conn id "smtp_alerts" (host, port, login, password)
  - Admin -> Variables:   "alert_email_to" (who receives alerts)
"""
import logging
import smtplib
from email.message import EmailMessage

import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable

logger = logging.getLogger(__name__)

SMTP_CONN_ID = "smtp_alerts"


def _send_email(subject, body):
    """Send a plain-text email using the SMTP settings in the Airflow connection."""
    # Read Variables/Connections inside the function, not at module level,
    # so the DB isn't hit every time Airflow parses the DAG file.
    to_addr = Variable.get("alert_email_to", default_var=None)
    if not to_addr:
        logger.warning("alert_email_to not set, skipping email alert")
        return

    conn = BaseHook.get_connection(SMTP_CONN_ID)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = conn.login
    msg["To"] = to_addr  # a comma-separated list works too
    msg.set_content(body)

    port = conn.port or 587
    if port == 465:
        server = smtplib.SMTP_SSL(conn.host, port, timeout=15)  # implicit TLS
    else:
        server = smtplib.SMTP(conn.host, port, timeout=15)
        server.starttls()  # upgrade to an encrypted connection before logging in

    with server:
        server.login(conn.login, conn.password)
        server.send_message(msg)


def notify_failure(context):
    """on_failure_callback: email when a task fails (after retries are used up)."""
    ti = context["task_instance"]

    subject = f"[Airflow] FAILED: {ti.dag_id}.{ti.task_id}"
    body = (
        f"An Airflow task failed.\n\n"
        f"DAG:   {ti.dag_id}\n"
        f"Task:  {ti.task_id}\n"
        f"Run:   {context['run_id']}\n"
        f"Try:   {ti.try_number}\n"
        f"Error: {context.get('exception')}\n\n"
        f"Logs:  {getattr(ti, 'log_url', 'not available')}\n"
    )

    try:
        _send_email(subject, body)
    except Exception:
        # Alerting must never become a new way for the pipeline to break
        logger.exception("Failed to send alert email")


def ping_heartbeat(variable_name="healthcheck_url"):
    """Ping an external monitor (e.g. Healthchecks.io) after a successful run.

    If the ping stops arriving, the monitor alerts you. This catches failures
    where nothing runs at all (Docker off, scheduler down, DAG paused).
    Use a different variable_name per pipeline.
    """
    url = Variable.get(variable_name, default_var=None)
    if not url:
        logger.warning("%s not set, skipping heartbeat", variable_name)
        return

    requests.get(url, timeout=10).raise_for_status()