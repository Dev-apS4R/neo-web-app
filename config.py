import os

# Email provider selection (MailerCloud only)
EMAIL_PROVIDER = "mailercloud"  # hardcoded to MailerCloud

# MailerCloud config
MAILER_CLOUD_CONFIG = {
    "api_key": os.getenv("UdYyf-93e7c4eaab787b17a6b553c3fe1af597-0b6d88cf891c6188b3978b8720f57d71", ""),
    "sender_email": os.getenv("neo.system@neoracer.in", "hello@neoracer.in"),
    "sender_name": os.getenv("NEO System", "NEO System"),
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

