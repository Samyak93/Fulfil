import csv
import os
from datetime import datetime
import requests
import logging
from django.conf import settings
from celery import shared_task, group, chord
from celery.exceptions import Retry
from .models import Product, Webhook
import redis

logger = logging.getLogger(__name__)
_r = redis.from_url(settings.REDIS_URL, decode_responses=True)

CHUNK_SIZE = 1000  # number of rows per subtask (can increase it to 5000 for more speed but use more memory)


@shared_task(bind=True, max_retries=3, default_retry_delay=10)
def import_products_task(self, filepath, job_id):
    """
    Master task: read CSV, dedupe globally (last occurrence wins), split into chunks,
    then dispatch parallel subtasks and a finalize callback.

    :param filepath: CSV filepath to process
    :param job_id: Current job_id
    :returns: JSON response of accepted/failed with error
    """
    try:
        _r.set(f"job:{job_id}:status", "parsing")
        _r.set(f"job:{job_id}:progress", 0)
        _r.set(f"job:{job_id}:message", "Parsing CSV")
        logger.info(f"Job {job_id}: started parsing CSV {filepath}")

        # --- GLOBAL DEDUPE: keep last occurrence of each SKU (case-insensitive) ---
        unique_map = {}  # sku_lower -> row dict (last seen wins!)
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
            logger.warning(f"Job {job_id}: CSV empty or invalid")
            return {'error': 'empty'}

        # init progress in redis for polling
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
        # actual celery async processing happens using below chord!
        header = group(process_batch.s(chunk, job_id, total_rows) for chunk in chunks)
        callback = finalize_import.s(job_id)
        chord(header)(callback)

        logger.info(f"Job {job_id}: dispatched {len(chunks)} chunks for processing")
        return {'ok': True}

    except Exception as exc:
        _r.set(f"job:{job_id}:status", "failed")
        _r.set(f"job:{job_id}:message", str(exc))
        logger.exception(f"Job {job_id} failed during import")
        try:
            # using exponential backoff, not using DLQ's as of now but can do it later!
            self.retry(exc=exc, countdown=2 ** self.request.retries)
        except Retry:
            logger.error(f"Job {job_id} exceeded max retries")
        return {'error': str(exc)}


@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def process_batch(self, batch, job_id, total_rows):
    """
    Subtask: process a batch of products.

    - Each batch contains globally unique products (deduped by SKU).
    - Performs bulk_create with update_conflicts=True (Postgres ON CONFLICT DO UPDATE).
    - Updates Redis with progress and status message.

    :param batch: list of dicts, each dict representing a product with 'sku', 'name', 'description'
    :param job_id: job ID for progress tracking in Redis
    :param total_rows: total number of products for calculating progress %
    :returns: number of products processed
    """
    try:
        curr_products = [
            Product(sku=r['sku'], name=r['name'], description=r['description'])
            for r in batch
        ]

        Product.objects.bulk_create(
            curr_products,
            update_conflicts=True,  # ON CONFLICT DO UPDATE
            update_fields=['name', 'description'],
            unique_fields=['sku'],
            batch_size=1000
        )

        processed = _r.incrby(f"job:{job_id}:processed", len(curr_products))
        _r.set(f"job:{job_id}:progress", int(processed * 100 / total_rows))
        _r.set(f"job:{job_id}:message", f"Processed {processed}/{total_rows}")
        logger.info(f"Job {job_id}: processed batch of {len(curr_products)} products")
        return len(curr_products)

    except Exception as exc:
        logger.exception(f"Job {job_id}: failed processing batch")
        try:
            # using exponential backoff, not using DLQ's as of now but can do it later!
            self.retry(exc=exc, countdown=2 ** self.request.retries)
        except Retry:
            logger.error(f"Job {job_id}: batch exceeded max retries")
        raise


@shared_task(bind=True)
def finalize_import(self, results, job_id):
    """
    Callback task after all batch subtasks finish.

    - Aggregates total processed count from subtasks.
    - Marks job as complete in Redis.
    - Triggers any product_imported webhooks.

    :param results: list of processed counts returned by each subtask
    :param job_id: job ID for progress tracking in Redis
    :returns: total number of products imported
    """
    try:
        total_processed = sum(results)
        _r.set(f"job:{job_id}:status", "complete")
        _r.set(f"job:{job_id}:progress", 100)
        _r.set(f"job:{job_id}:message", f"Import complete ({total_processed} products)")
        logger.info(f"Job {job_id}: import complete ({total_processed} products)")
        _trigger_webhooks('product_imported', {'total_imported': total_processed})
        return total_processed
    except Exception as e:
        logger.exception(f"Job {job_id}: finalize import failed")
        _r.set(f"job:{job_id}:status", "failed")
        _r.set(f"job:{job_id}:message", str(e))
        return {'error': str(e)}


def _trigger_webhooks(event, payload):
    """
    Send HTTP POST requests to all enabled webhooks for the given event.

    :param event: event type (string)
    :param payload: dict with event payload
    """
    hooks = Webhook.objects.filter(event_type=event, is_enabled=True)
    for h in hooks:
        try:
            resp = requests.post(h.url, json={'event': event, 'payload': payload}, timeout=5)
            logger.info(f"Webhook triggered: {h.url} status={resp.status_code}")
        except Exception as e:
            logger.error(f"Webhook failed: {h.url} error={str(e)}")


@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def test_webhook_task(self, hook_id):
    """
    Task to test a webhook endpoint.

    - Sends a test payload to the webhook URL.
    - Updates Webhook model with last_test_status and last_tested_at.

    :param hook_id: ID of the webhook to test
    :returns: dict with HTTP status code or error
    """
    try:
        hook = Webhook.objects.get(id=hook_id)
        resp = requests.post(hook.url, json={'test': True}, timeout=10)
        hook.last_test_status = resp.status_code
        hook.last_tested_at = datetime.utcnow()
        hook.save()
        logger.info(f"Webhook test success: {hook.url} status={resp.status_code}")
        return {'status': resp.status_code}
    except Exception as exc:
        logger.exception(f"Webhook test failed: {hook_id}")
        try:
            # using exponential backoff, not using DLQ's as of now but can do it later!
            self.retry(exc=exc, countdown=2 ** self.request.retries)
        except Retry:
            logger.error(f"Webhook test retry exceeded: {hook_id}")
        return {'error': str(exc)}
