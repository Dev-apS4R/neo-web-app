import os

# Email provider selection (MailerCloud only)
EMAIL_PROVIDER = "mailercloud"  # hardcoded to MailerCloud

# MailerCloud config
MAILER_CLOUD_CONFIG = {
    "api_key": os.getenv("NEO_MAILERCLOUD_API_KEY", ""),
    "sender_email": os.getenv("NEO_MAILERCLOUD_SENDER_EMAIL", "hello@neoracer.in"),
    "sender_name": os.getenv("NEO_MAILERCLOUD_SENDER_NAME", "NEO System"),
    "enabled": os.getenv("NEO_MAILERCLOUD_ENABLED", "false").lower() == "true"
}

# Unified email config - MailerCloud only
def get_email_config():
    return {
        "provider": "mailercloud",
        **MAILER_CLOUD_CONFIG
    }

EMAIL_CONFIG = get_email_config()

# For MailerCloud:
# NEO_EMAIL_PROVIDER=mailercloud
# NEO_MAILERCLOUD_ENABLED=true
# NEO_MAILERCLOUD_API_KEY=your_api_key_here
# NEO_MAILERCLOUD_SENDER_EMAIL=verified_sender@yourdomain.com
# NEO_MAILERCLOUD_SENDER_NAME=NEO System
