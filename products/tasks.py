import csv
from datetime import datetime
import requests
from django.conf import settings
from celery import shared_task
from .models import Product, Webhook
import redis

_r = redis.from_url(settings.REDIS_URL, decode_responses=True)

CHUNK_SIZE = 1000

@shared_task
def import_products_task(filepath, job_id):
    try:
        _r.set(f"job:{job_id}:status", "parsing")
        # count rows (skip header if present)
        with open(filepath, encoding='utf-8', errors='ignore') as f:
            total_rows = sum(1 for _ in f) - 1  # heuristic, if header exists
        if total_rows <= 0:
            _r.set(f"job:{job_id}:status", "failed")
            _r.set(f"job:{job_id}:message", "Empty file or invalid")
            return {'error': 'empty'}

        processed = 0
        _r.set(f"job:{job_id}:status", "processing")
        _r.set(f"job:{job_id}:progress", 0)
        _r.set(f"job:{job_id}:message", "Starting import")

        with open(filepath, newline='', encoding='utf-8', errors='ignore') as csvfile:
            reader = csv.DictReader(csvfile)
            batch = []
            for row in reader:
                # normalize headerless rows: skip if blank SKU
                sku = (row.get('sku') or row.get('SKU') or row.get('Sku') or '').strip()
                # if header row included, skip it
                if sku.lower() == 'sku' or sku == '':
                    continue
                batch.append({
                    'sku': sku,
                    'name': (row.get('name') or row.get('Name') or '').strip(),
                    'description': (row.get('description') or row.get('Description') or '').strip(),
                })
                if len(batch) >= CHUNK_SIZE:
                    _process_batch(batch)
                    processed += len(batch)
                    batch = []
                    _r.set(f"job:{job_id}:progress", int(processed * 100 / total_rows))
                    _r.set(f"job:{job_id}:message", f"Processed {processed}/{total_rows}")
            if batch:
                _process_batch(batch)
                processed += len(batch)
                _r.set(f"job:{job_id}:progress", int(processed * 100 / total_rows))
                _r.set(f"job:{job_id}:message", f"Processed {processed}/{total_rows}")

        _r.set(f"job:{job_id}:status", "complete")
        _r.set(f"job:{job_id}:progress", 100)
        _r.set(f"job:{job_id}:message", "Import complete")
        _trigger_webhooks('product_imported', {'total': total_rows, 'imported': processed})
        return {'ok': True}
    except Exception as e:
        _r.set(f"job:{job_id}:status", "failed")
        _r.set(f"job:{job_id}:message", str(e))
        return {'error': str(e)}

def _process_batch(batch):
    """
    Upsert each row case-insensitively on SKU.
    For simplicity we do per-row get/update/create.
    This is reliable and clear for the take-home. For 500k rows,
    consider replacing with psycopg2 bulk INSERT ... ON CONFLICT for speed.
    """
    for r in batch:
        sku = r['sku']
        name = r['name']
        desc = r['description']
        try:
            existing = Product.objects.get(sku__iexact=sku)
            existing.name = name
            existing.description = desc
            existing.sku = sku  # keep canonical casing from CSV
            existing.save()
        except Product.DoesNotExist:
            Product.objects.create(sku=sku, name=name, description=desc)

def _trigger_webhooks(event, payload):
    hooks = Webhook.objects.filter(event_type=event, is_enabled=True)
    for h in hooks:
        try:
            requests.post(h.url, json={'event': event, 'payload': payload}, timeout=5)
        except Exception:
            pass

@shared_task
def test_webhook_task(hook_id):
    try:
        hook = Webhook.objects.get(id=hook_id)
        resp = requests.post(hook.url, json={'test': True}, timeout=10)
        hook.last_test_status = resp.status_code
        hook.last_tested_at = datetime.utcnow()
        hook.save()
        return {'status': resp.status_code}
    except Exception as e:
        return {'error': str(e)}
