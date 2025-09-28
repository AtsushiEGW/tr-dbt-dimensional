import os

# ===== Security =====
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "please-change-me")

# ===== Metadata DB =====
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_DATABASE_URI")

# ===== Redis (Cache & Celery broker/result) =====
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"

# Cache (ダッシュボード/クエリ高速化)
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": f"{REDIS_URL}/1",
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# Celery（非同期処理 / Alerts & Reports 等）
class CeleryConfig:
    broker_url = f"{REDIS_URL}/0"
    result_backend = f"{REDIS_URL}/0"
    task_annotations = {"*": {"rate_limit": "10/s"}}
    worker_send_task_events = True
    task_send_sent_event = True

CELERY_CONFIG = CeleryConfig

# =====（任意）機能フラグ =====
# Alerts & Reports を使う場合に有効化（ヘッドレスブラウザ準備も必要）
FEATURE_FLAGS = {
    # "ALERT_REPORTS": True,
}

# =====（任意）メール送信（レポート配信に利用） =====
# EMAIL_NOTIFICATIONS = True
# SMTP_HOST = "smtp.example.com"
# SMTP_PORT = 587
# SMTP_STARTTLS = True
# SMTP_SSL = False
# SMTP_USER = "user"
# SMTP_PASSWORD = "password"
# SMTP_MAIL_FROM = "no-reply@example.com"

# =====（任意）レポート画像レンダリング用 WebDriver =====
# REPORTS_WEBDRIVER = "chrome"
# WEBDRIVER_BASEURL = os.environ.get("WEBDRIVER_BASEURL", "http://superset:8088/")
# WEBDRIVER_TYPE = "chrome"
# WEBDRIVER_OPTION_ARGS = ["--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]