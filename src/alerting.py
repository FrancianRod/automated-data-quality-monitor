"""
=============================================================
Alert Manager — Email & Telegram
Sends automated alerts with zero manual intervention.
=============================================================
"""

import smtplib
import os
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

logger = logging.getLogger(__name__)


class AlertManager:
    """
    Routes alerts to configured channels (Telegram, Email, or both).
    Designed for silent, autonomous operation — no human trigger needed.
    """

    def __init__(self, config: dict):
        self.channels = []

        if config.get("telegram", {}).get("enabled"):
            self.channels.append(TelegramAlert(config["telegram"]))

        if config.get("email", {}).get("enabled"):
            self.channels.append(EmailAlert(config["email"]))

    def send(self, health_result) -> None:
        for channel in self.channels:
            try:
                channel.send(health_result)
            except Exception as e:
                logger.error(f"[AlertManager] Failed to send via {channel.__class__.__name__}: {e}")


# ─────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────
class TelegramAlert:
    def __init__(self, config: dict):
        self.token = config.get("bot_token") or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = config.get("chat_id") or os.getenv("TELEGRAM_CHAT_ID")

    def send(self, health) -> None:
        if not REQUESTS_AVAILABLE:
            logger.warning("requests library not available. Skipping Telegram alert.")
            return

        emoji = "🔴" if health.overall_status == "CRITICAL" else "🟡"
        lines = [
            f"{emoji} *Pipeline Monitor Alert*",
            f"*Pipeline:* {health.pipeline_name}",
            f"*Table:* `{health.table_name}`",
            f"*Status:* {health.overall_status}",
            f"*Run at:* {health.run_at}",
            f"*Rows analyzed:* {health.total_rows:,}",
            f"*Checks failed:* {health.checks_failed}/{health.checks_passed + health.checks_failed}",
            "",
            "*Issues Found:*",
        ]

        for issue in health.issues[:5]:  # cap at 5 to avoid flooding
            lines.append(f"  ⚠️ [{issue['check']}] `{issue['column']}` — {issue['detail']}")

        if len(health.issues) > 5:
            lines.append(f"  _...and {len(health.issues) - 5} more issues._")

        message = "\n".join(lines)

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info(f"[Telegram] Alert sent. Status: {resp.status_code}")


# ─────────────────────────────────────────
# Email
# ─────────────────────────────────────────
class EmailAlert:
    def __init__(self, config: dict):
        self.smtp_host = config.get("smtp_host", "smtp.gmail.com")
        self.smtp_port = config.get("smtp_port", 587)
        self.sender = config.get("sender") or os.getenv("EMAIL_SENDER")
        self.password = config.get("password") or os.getenv("EMAIL_PASSWORD")
        self.recipients = config.get("recipients", [])

    def send(self, health) -> None:
        subject = f"[{health.overall_status}] Pipeline Alert — {health.pipeline_name} ({health.table_name})"
        body = self._build_html(health)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.sender, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())

        logger.info(f"[Email] Alert sent to {self.recipients}")

    def _build_html(self, health) -> str:
        status_color = "#e74c3c" if health.overall_status == "CRITICAL" else "#f39c12"
        rows = ""
        for issue in health.issues:
            sev_color = "#e74c3c" if issue["severity"] == "CRITICAL" else "#f39c12"
            rows += f"""
            <tr>
              <td>{issue['check']}</td>
              <td><code>{issue['column']}</code></td>
              <td style="color:{sev_color};font-weight:bold">{issue['severity']}</td>
              <td>{issue['detail']}</td>
            </tr>"""

        return f"""
        <html><body style="font-family:Arial,sans-serif;padding:20px;color:#2c3e50">
          <h2 style="color:{status_color}">⚠️ Pipeline Monitor — {health.overall_status}</h2>
          <table style="border-collapse:collapse;margin-bottom:16px">
            <tr><td><b>Pipeline:</b></td><td>{health.pipeline_name}</td></tr>
            <tr><td><b>Table:</b></td><td>{health.table_name}</td></tr>
            <tr><td><b>Run at:</b></td><td>{health.run_at}</td></tr>
            <tr><td><b>Rows analyzed:</b></td><td>{health.total_rows:,}</td></tr>
            <tr><td><b>Checks failed:</b></td><td>{health.checks_failed} / {health.checks_passed + health.checks_failed}</td></tr>
          </table>
          <h3>Issues Found</h3>
          <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;width:100%">
            <thead style="background:#2c3e50;color:white">
              <tr><th>Check</th><th>Column</th><th>Severity</th><th>Detail</th></tr>
            </thead>
            <tbody>{rows}</tbody>
          </table>
          <p style="color:#7f8c8d;font-size:12px;margin-top:24px">
            Automated alert by Data Pipeline Monitor · Zero manual intervention
          </p>
        </body></html>
        """
