"""
NEO EMAIL SYSTEM — Asynchronous Email Processing with Jinja2 Templating
Handles email queuing, templating, workflows, and delivery tracking.
"""

import threading
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional
import json
from jinja2 import Template
import os


class NeoEmailSystem:
    """
    Asynchronous email system with queuing, templating, and workflows.
    Singleton pattern to ensure one worker thread per application.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, vault, email_config):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(NeoEmailSystem, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, vault, email_config):
        if self._initialized:
            return

        self.vault = vault
        self.email_config = email_config
        self.running = False
        self.worker_thread = None
        self.templates = {}  # Cache for loaded templates

        # Load default templates
        self._load_default_templates()

        # Load default workflows
        self._load_default_workflows()

        self._initialized = True

    def start(self):
        """Start the email worker thread."""
        if self.running:
            return

        self.running = True
        self.worker_thread = threading.Thread(target=self._email_worker, daemon=True)
        self.worker_thread.start()
        print("[EMAIL] Email worker started")

    def stop(self):
        """Stop the email worker thread."""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        print("[EMAIL] Email worker stopped")

    def _email_worker(self):
        """Background worker that processes email queue."""
        while self.running:
            try:
                pending_emails = self.vault.get_pending_emails(limit=5)

                for email_data in pending_emails:
                    email_id, to_email, subject, html_body, text_body, template_name, template_data_json, \
                    priority, attempts, max_attempts, user_id = email_data

                    try:
                        self.vault.mark_email_sending(email_id)

                        # Render template if needed
                        if template_name and not html_body:
                            subject, html_body, text_body = self._render_template(
                                template_name, json.loads(template_data_json) if template_data_json else {}
                            )

                        # Send email
                        self._send_email(to_email, subject, html_body, text_body)

                        # Track successful delivery
                        self.vault.mark_email_sent(email_id)
                        self.vault.track_email_event(email_id, "delivered")
                        print(f"[EMAIL] Sent email to {to_email}: {subject}")

                    except Exception as e:
                        error_msg = str(e)
                        self.vault.mark_email_failed(email_id, error_msg)
                        self.vault.track_email_event(email_id, "failed", {"error": error_msg})
                        print(f"[EMAIL] Failed to send email {email_id}: {error_msg}")

                # Sleep before next batch
                time.sleep(10)  # Check every 10 seconds

            except Exception as e:
                print(f"[EMAIL] Worker error: {e}")
                time.sleep(30)  # Back off on errors

    def _send_email(self, to_email: str, subject: str, html_body: str, text_body: str):
        """Send email via SMTP."""
        if not self.email_config.get("enabled", False):
            raise Exception("Email system disabled")

        smtp_server = self.email_config["smtp_server"]
        smtp_port = self.email_config["smtp_port"]
        sender_email = self.email_config["sender_email"]
        sender_password = self.email_config["sender_password"]

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = to_email

        if text_body:
            msg.attach(MIMEText(text_body, 'plain'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html'))

        server = smtplib.SMTP(smtp_server, smtp_port)
        try:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, msg.as_string())
        finally:
            server.quit()

    def _load_default_templates(self):
        """Load default email templates."""
        templates = {
            "verification": {
                "subject": "NEO Account Verification - {{ username }}",
                "html_template": """
                <html>
                <head>
                    <style>
                        body { font-family: 'Courier New', monospace; background: #000; color: #00f2ff; padding: 20px; }
                        .container { max-width: 600px; margin: 0 auto; background: #111; border: 2px solid #00f2ff; border-radius: 10px; padding: 30px; }
                        .header { text-align: center; font-size: 36px; color: #ff00ff; margin-bottom: 20px; }
                        .code { font-size: 24px; color: #ff6600; font-weight: bold; text-align: center; margin: 20px 0; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">N E O</div>
                        <h2>Verify Your Account</h2>
                        <p>Hello {{ username }},</p>
                        <p>Your verification code is:</p>
                        <div class="code">{{ code }}</div>
                        <p>Enter this code in the app to activate your account.</p>
                        <p>Welcome to the neural network.</p>
                    </div>
                </body>
                </html>
                """,
                "text_template": """
                NEO Account Verification

                Hello {{ username }},

                Your verification code is: {{ code }}

                Enter this code in the app to activate your account.

                Welcome to the neural network.
                """
            },
            "password_recovery": {
                "subject": "NEO Password Recovery",
                "html_template": """
                <html>
                <head>
                    <style>
                        body { font-family: 'Courier New', monospace; background: #000; color: #00f2ff; padding: 20px; }
                        .container { max-width: 600px; margin: 0 auto; background: #111; border: 2px solid #00f2ff; border-radius: 10px; padding: 30px; }
                        .header { text-align: center; font-size: 36px; color: #ff00ff; margin-bottom: 20px; }
                        .code { font-size: 24px; color: #ff6600; font-weight: bold; text-align: center; margin: 20px 0; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">N E O</div>
                        <h2>Password Recovery</h2>
                        <p>Your recovery code is:</p>
                        <div class="code">{{ code }}</div>
                        <p>Use this code in the app to reset your password. Code expires in 1 hour.</p>
                    </div>
                </body>
                </html>
                """,
                "text_template": """
                NEO Password Recovery

                Your recovery code is: {{ code }}

                Use this code in the app to reset your password. Code expires in 1 hour.
                """
            },
            "welcome": {
                "subject": "Welcome to NEO - Complete Guide",
                "html_template": """
                <html>
                <head>
                    <style>
                        body { font-family: 'Courier New', monospace; background: #000; color: #00f2ff; padding: 20px; }
                        .container { max-width: 600px; margin: 0 auto; background: #111; border: 2px solid #00f2ff; border-radius: 10px; padding: 30px; }
                        .header { text-align: center; font-size: 48px; color: #ff00ff; text-shadow: 0 0 10px #00f2ff; margin-bottom: 20px; }
                        .section { margin: 20px 0; padding: 15px; background: #222; border-left: 4px solid #ff6600; }
                        .highlight { color: #ff6600; font-weight: bold; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">N E O</div>
                        <div class="section">
                            <h2>Welcome to NEO, {{ username }}!</h2>
                            <p>Thank you for choosing the Neural Environment Operator. You're now part of the future of AI-assisted driving.</p>
                        </div>
                        <div class="section">
                            <h3>Getting Started</h3>
                            <p>Launch NEO after logging in. Select your game, calibrate, and choose an AI mode:</p>
                            <ul>
                                <li><span class="highlight">Self-Drive:</span> Full AI control</li>
                                <li><span class="highlight">Assist:</span> AI helps your driving</li>
                                <li><span class="highlight">Safety:</span> AI intervenes in danger</li>
                            </ul>
                        </div>
                        <div class="section">
                            <h3>Security Reminder</h3>
                            <p>Your data is secure. Use strong passwords and enable 2FA when available.</p>
                        </div>
                        <div class="footer">
                            <p>Thank you for using NEO. Welcome to the neural network.</p>
                            <p>- NEO Development Team</p>
                        </div>
                    </div>
                </body>
                </html>
                """,
                "text_template": """
                Welcome to NEO, {{ username }}!

                Thank you for choosing the Neural Environment Operator. You're now part of the future of AI-assisted driving.

                Getting Started:
                Launch NEO after logging in. Select your game, calibrate, and choose an AI mode:
                - Self-Drive: Full AI control
                - Assist: AI helps your driving
                - Safety: AI intervenes in danger

                Security Reminder:
                Your data is secure. Use strong passwords and enable 2FA when available.

                Thank you for using NEO. Welcome to the neural network.
                - NEO Development Team
                """
            },
            "login_notification": {
                "subject": "NEO Login Notification",
                "html_template": """
                <html>
                <head>
                    <style>
                        body { font-family: 'Courier New', monospace; background: #000; color: #00f2ff; padding: 20px; }
                        .container { max-width: 600px; margin: 0 auto; background: #111; border: 2px solid #00f2ff; border-radius: 10px; padding: 30px; }
                        .header { text-align: center; font-size: 36px; color: #ff00ff; margin-bottom: 20px; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">N E O</div>
                        <h2>Login Notification</h2>
                        <p>Hello {{ username }},</p>
                        <p>You have successfully logged into NEO at {{ timestamp }}.</p>
                        <p>If this wasn't you, please contact support immediately.</p>
                    </div>
                </body>
                </html>
                """,
                "text_template": """
                NEO Login Notification

                Hello {{ username }},

                You have successfully logged into NEO at {{ timestamp }}.

                If this wasn't you, please contact support immediately.
                """
            }
        }

        for name, template_data in templates.items():
            self.vault.save_email_template(
                name=name,
                subject=template_data["subject"],
                html_template=template_data["html_template"],
                text_template=template_data["text_template"]
            )

    def _load_default_workflows(self):
        """Load default email workflows."""
        workflows = {
            "user_registered": {
                "description": "Welcome series for new users",
                "steps": [
                    {
                        "template_name": "welcome",
                        "delay_hours": 0,
                        "priority": 2
                    }
                ]
            },
            "user_verified": {
                "description": "Post-verification welcome",
                "steps": [
                    {
                        "template_name": "welcome",
                        "delay_hours": 0,
                        "priority": 1
                    }
                ]
            }
        }

        for trigger_event, workflow_data in workflows.items():
            self.vault.save_email_workflow(
                name=f"{trigger_event}_workflow",
                description=workflow_data["description"],
                trigger_event=trigger_event,
                steps=workflow_data["steps"]
            )

    def _render_template(self, template_name: str, context: Dict[str, Any]) -> tuple[str, str, str]:
        """Render a template with context data."""
        template = self.vault.get_email_template(template_name)
        if not template:
            raise Exception(f"Template {template_name} not found")

        # Render subject
        subject_template = Template(template["subject"])
        subject = subject_template.render(**context)

        # Render HTML body
        html_template = Template(template["html_template"])
        html_body = html_template.render(**context)

        # Render text body
        text_body = ""
        if template["text_template"]:
            text_template = Template(template["text_template"])
            text_body = text_template.render(**context)

        return subject, html_body, text_body

    def send_template_email(self, to_email: str, template_name: str, context: Dict[str, Any],
                          priority: int = 1, user_id: Optional[int] = None):
        """Queue an email using a template."""
        return self.vault.queue_email(
            to_email=to_email,
            subject="",  # Will be filled by template
            template_name=template_name,
            template_data=context,
            priority=priority,
            user_id=user_id
        )

    def trigger_workflow(self, trigger_event: str, user_id: int, context: Optional[Dict[str, Any]] = None):
        """Trigger an email workflow."""
        self.vault.trigger_workflow(trigger_event, user_id, context)

    def get_email_performance_stats(self) -> Dict[str, Any]:
        """Get email performance statistics."""
        return self.vault.get_email_performance_stats()

    def get_email_stats(self, user_id: int = None) -> Dict[str, Any]:
        """Get email delivery statistics."""
        return self.vault.get_email_stats(user_id)
