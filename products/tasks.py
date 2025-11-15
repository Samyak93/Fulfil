import csv
import os
from datetime import datetime
import requests
from django.conf import settings
from celery import shared_task, group, chord
from .models import Product, Webhook
import redis

_r = redis.from_url(settings.REDIS_URL, decode_responses=True)

CHUNK_SIZE = 1000  # number of rows per subtask

@shared_task
def import_products_task(filepath, job_id):
    """
    Master task: splits CSV into chunks and triggers subtasks in parallel
    """
    try:
        _r.set(f"job:{job_id}:status", "parsing")
        with open(filepath, encoding='utf-8', errors='ignore') as f:
            total_rows = sum(1 for _ in f) - 1
        if total_rows <= 0:
            _r.set(f"job:{job_id}:status", "failed")
            _r.set(f"job:{job_id}:message", "Empty file or invalid")
            return {'error': 'empty'}

        _r.set(f"job:{job_id}:status", "processing")
        _r.set(f"job:{job_id}:progress", 0)
        _r.set(f"job:{job_id}:message", "Starting import")

        # Read CSV and create chunks
        chunks = []
        batch = []
        with open(filepath, newline='', encoding='utf-8', errors='ignore') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                sku = (row.get('sku') or row.get('SKU') or row.get('Sku') or '').strip()
                if sku.lower() == 'sku' or sku == '':
                    continue
                batch.append({
                    'sku': sku,
                    'name': (row.get('name') or row.get('Name') or '').strip(),
                    'description': (row.get('description') or row.get('Description') or '').strip(),
                })
                if len(batch) >= CHUNK_SIZE:
                    chunks.append(batch)
                    batch = []
            if batch:
                chunks.append(batch)

        # Use chord to run all batches in parallel and then a callback
        header = group(process_batch.s(chunk, job_id, total_rows) for chunk in chunks)
        callback = finalize_import.s(job_id)
        chord(header)(callback)
        return {'ok': True}

    except Exception as e:
        _r.set(f"job:{job_id}:status", "failed")
        _r.set(f"job:{job_id}:message", str(e))
        return {'error': str(e)}


@shared_task
def process_batch(batch, job_id, total_rows):
    """
    Subtask to process a single batch of products
    """
    for r in batch:
        sku = r['sku']
        name = r['name']
        desc = r['description']
        try:
            existing = Product.objects.get(sku__iexact=sku)
            existing.name = name
            existing.description = desc
            existing.save()
        except Product.DoesNotExist:
            Product.objects.create(sku=sku, name=name, description=desc)

    # Update progress in Redis
    processed = int(_r.get(f"job:{job_id}:processed") or 0) + len(batch)
    _r.set(f"job:{job_id}:processed", processed)
    _r.set(f"job:{job_id}:progress", int(processed * 100 / total_rows))
    _r.set(f"job:{job_id}:message", f"Processed {processed}/{total_rows}")
    return processed


@shared_task
def finalize_import(results, job_id):
    """
    Callback task after all chunks finish
    """
    total_processed = sum(results)
    _r.set(f"job:{job_id}:status", "complete")
    _r.set(f"job:{job_id}:progress", 100)
    _r.set(f"job:{job_id}:message", f"Import complete ({total_processed} products)")
    _trigger_webhooks('product_imported', {'total_imported': total_processed})
    return total_processed


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
