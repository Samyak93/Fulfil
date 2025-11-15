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
    Master task: read CSV, dedupe globally (last occurrence wins), split into chunks,
    then dispatch parallel subtasks and a finalize callback.
    """
    try:
        _r.set(f"job:{job_id}:status", "parsing")
        _r.set(f"job:{job_id}:progress", 0)
        _r.set(f"job:{job_id}:message", "Parsing CSV")

        # --- GLOBAL DEDUPE: keep last occurrence of each SKU (case-insensitive) ---
        unique_map = {}  # sku_lower -> row dict (last seen wins)
        with open(filepath, newline='', encoding='utf-8', errors='ignore') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                sku_raw = (row.get('sku') or row.get('SKU') or row.get('Sku') or '').strip()
                if not sku_raw:
                    continue
                sku = sku_raw.lower()
                if sku == 'sku':  # skip header-like rows
                    continue
                unique_map[sku] = {
                    'sku': sku,  # normalized to lowercase for DB unique index
                    'name': (row.get('name') or row.get('Name') or '').strip(),
                    'description': (row.get('description') or row.get('Description') or '').strip(),
                }

        unique_rows = list(unique_map.values())
        total_rows = len(unique_rows)

        if total_rows <= 0:
            _r.set(f"job:{job_id}:status", "failed")
            _r.set(f"job:{job_id}:message", "Empty file or invalid")
            return {'error': 'empty'}

        # init progress
        _r.set(f"job:{job_id}:status", "processing")
        _r.set(f"job:{job_id}:progress", 0)
        _r.set(f"job:{job_id}:message", "Starting import")
        _r.set(f"job:{job_id}:processed", 0)
        _r.set(f"job:{job_id}:total", total_rows)

        # --- CHUNKING ---
        chunks = []
        batch = []
        for r in unique_rows:
            batch.append(r)
            if len(batch) >= CHUNK_SIZE:
                chunks.append(batch)
                batch = []
        if batch:
            chunks.append(batch)

        # dispatch tasks in parallel and finalize
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
    Subtask to process a single batch of products — batch items are already globally unique.
    Uses a single bulk_create with update_conflicts=True (Postgres ON CONFLICT DO UPDATE).
    """
    # prepare product instances (sku already normalized to lowercase in import step)
    curr_products = [
        Product(
            sku=r['sku'],
            name=r['name'],
            description=r['description']
        )
        for r in batch
    ]

    # Bulk upsert in one DB statement (Django + Postgres)
    Product.objects.bulk_create(
        curr_products,
        update_conflicts=True,              # ON CONFLICT DO UPDATE
        update_fields=['name', 'description'],
        unique_fields=['sku'],
        batch_size=1000
    )

    # Atomically increment processed count (avoid race conditions across workers)
    processed = _r.incrby(f"job:{job_id}:processed", len(curr_products))
    # Update progress and message
    _r.set(f"job:{job_id}:progress", int(processed * 100 / total_rows))
    _r.set(f"job:{job_id}:message", f"Processed {processed}/{total_rows}")

    # Return how many items this task processed (used by chord)
    return len(curr_products)


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
