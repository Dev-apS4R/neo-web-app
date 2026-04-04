import os

# Email config for local testing
EMAIL_CONFIG = {
    "sender_email": os.getenv("NEO_EMAIL", "test@example.com"),
    "sender_password": os.getenv("NEO_EMAIL_PASS", "testpass"),
    "smtp_server": os.getenv("NEO_SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("NEO_SMTP_PORT", 587)),
    "enabled": os.getenv("NEO_EMAIL_ENABLED", "false").lower() == "true"
}

# For local testing without real email, you can enable and use Gmail
# Set these environment variables:
# NEO_EMAIL_ENABLED=true
# NEO_EMAIL=your_gmail@gmail.com
# NEO_EMAIL_PASS=your_app_password (not regular password - generate app password in Gmail settings)