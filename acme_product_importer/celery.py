import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'acme_product_importer.settings')

app = Celery('acme_product_importer')

# Load any custom config from Django settings prefixed with CELERY_
app.config_from_object('django.conf:settings', namespace='CELERY')

# Broker and result backend from REDIS_URL
app.conf.broker_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
app.conf.result_backend = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# Accept JSON only
app.conf.accept_content = ['json']
app.conf.task_serializer = 'json'
app.conf.result_serializer = 'json'

# Enable events for worker discovery / monitoring
app.conf.worker_send_task_events = True
app.conf.task_send_sent_event = True
app.conf.worker_enable_remote_control = True

# Autodiscover tasks.py in installed apps
app.autodiscover_tasks()
