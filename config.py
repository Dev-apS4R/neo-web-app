import os

# Email provider selection
EMAIL_PROVIDER = "smtp"

# Generic SMTP config
SMTP_CONFIG = {
    "smtp_server": os.getenv("NEO_SMTP_SERVER", ""),
    "smtp_port": int(os.getenv("NEO_SMTP_PORT", 587)),
    "sender_email": os.getenv("NEO_SENDER_EMAIL", "hello@neoracer.in"),
    "sender_password": os.getenv("NEO_SENDER_PASSWORD", ""),
    "enabled": os.getenv("NEO_EMAIL_ENABLED", "true").lower() == "true"
}

# Unified email config
def get_email_config():
    return {
        "provider": "smtp",
        **SMTP_CONFIG
    }

EMAIL_CONFIG = get_email_config()
