import os
from cachelib.redis import RedisCache
from celery.schedules import crontab

# =====================================================
# Superset Database Connection (Postgres for metadata)
# =====================================================
SQLALCHEMY_DATABASE_URI = os.getenv(
    "DATABASE_URL",
    "postgresql://superset:superset@postgres-superset:5432/superset_metadata"
)

# =====================================================
# Redis Config
# =====================================================
REDIS_HOST = os.getenv("REDIS_HOST", "superset_redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

# Redis DB separation
REDIS_CELERY_DB = os.getenv("REDIS_CELERY_DB", "0")
REDIS_RESULTS_DB = os.getenv("REDIS_RESULTS_DB", "1")
REDIS_CACHE_DB = os.getenv("REDIS_CACHE_DB", "2")  # SQL Lab + Charts cache

# =====================================================
# Celery Config (Alerts & Reports scheduling)
# =====================================================
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    broker_connection_retry_on_startup = True

    imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
    )

    worker_prefetch_multiplier = 10
    task_acks_late = True
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
    }

    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=0, hour=0),
        },
    }

CELERY_CONFIG = CeleryConfig

# =====================================================
# Superset Results Backend (required!)
# =====================================================
RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=REDIS_PORT,
    key_prefix="superset_results_",
    db=REDIS_RESULTS_DB,
    default_timeout=3600,  # 1 hour cache
)

# =====================================================
# Superset Caching (SQL Lab & Charts)
# =====================================================
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_CACHE_DB,
}

# =====================================================
# Superset Security & Defaults
# =====================================================
SECRET_KEY = os.getenv("SECRET_KEY", "thisISaSECRET_1234")
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
}

SUPERSET_WEBSERVER_TIMEOUT = int(os.getenv("SUPERSET_WEBSERVER_TIMEOUT", "60"))

# =====================================================
# Email (SendGrid Example for Alerts/Reports)
# =====================================================
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = "ziadashraf98765@gmail.com"     # your Gmail
SMTP_PASSWORD = "bqdv sxlf txat hfqs"   # Not your Gmail password!
SMTP_MAIL_FROM = "ziadashraf98765@gmail.com"

# All reports/alerts will be sent here
ALERT_REPORTS_NOTIFICATION_EMAIL = "ziadashraf98765@gmail.com"
EMAIL_REPORTS_SUBJECT_PREFIX = "[Superset] "

# =====================================================
# Screenshot & WebDriver (for charts in email/Slack)
# =====================================================
SCREENSHOT_LOCATE_WAIT = 100
SCREENSHOT_LOAD_WAIT = 600

# WEBDRIVER_TYPE = "chrome"
# WEBDRIVER_OPTION_ARGS = [
#     "--force-device-scale-factor=2.0",
#     "--high-dpi-support=2.0",
#     "--headless",
#     "--disable-gpu",
#     "--disable-dev-shm-usage",
#     "--no-sandbox",
#     "--disable-setuid-sandbox",
#     "--disable-extensions",
# ]

# Internal + external URL mapping
WEBDRIVER_BASEURL = "http://superset:8088"  
WEBDRIVER_BASEURL_USER_FRIENDLY = "http://localhost:8088"

# =====================================================
# Slack Alerts (Optional)
# =====================================================
SLACK_API_TOKEN = os.getenv("SLACK_API_TOKEN", "xoxb-xxxxxx")
