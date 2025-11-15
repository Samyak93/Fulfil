import os
import uuid
import redis
import logging
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.conf import settings
from django.db.models import Q
from django.views.decorators.http import require_POST
from .forms import UploadFileForm, ProductForm, WebhookForm
from .models import Product, Webhook
from .tasks import import_products_task, test_webhook_task, _trigger_webhooks

# Redis connection for job progress
_r = redis.from_url(settings.REDIS_URL, decode_responses=True)
logger = logging.getLogger(__name__)


def index(request):
    """
    Render the home page with file upload form.
    """
    form = UploadFileForm()
    return render(request, 'products/index.html', {'form': form})


def upload_file(request):
    """
    Handle CSV file upload, save to disk, initialize Redis job,
    and enqueue async import task.
    """
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            try:
                f = form.cleaned_data['file']
                # Saving csv to media folder
                job_id = str(uuid.uuid4())
                upload_dir = os.path.join(settings.MEDIA_ROOT or 'media', 'uploads')
                os.makedirs(upload_dir, exist_ok=True)
                filepath = os.path.join(upload_dir, f"{job_id}.csv")
                with open(filepath, 'wb') as dest:
                    for chunk in f.chunks():
                        dest.write(chunk)

                # init Redis progress & save filepath for retry
                _r.set(f"job:{job_id}:status", "queued")
                _r.set(f"job:{job_id}:progress", 0)
                _r.set(f"job:{job_id}:message", "Queued")
                _r.set(f"job:{job_id}:processed", 0)
                _r.set(f"job:{job_id}:filepath", filepath)

                # enqueue async task
                import_products_task.delay(filepath, job_id)
                logger.info(f"File uploaded and job queued: {job_id}")
                return JsonResponse({'job_id': job_id})
            except Exception as e:
                logger.exception("Error during file upload")
                return JsonResponse({'error': str(e)}, status=500)
    return JsonResponse({'error': 'invalid'}, status=400)


def job_status(request, job_id):
    """
    Return JSON with current progress and status of job.
    """
    try:
        status = _r.get(f"job:{job_id}:status") or 'unknown'
        progress = int(_r.get(f"job:{job_id}:progress") or 0)
        msg = _r.get(f"job:{job_id}:message") or ''
        return JsonResponse({'status': status, 'progress': progress, 'message': msg})
    except Exception as e:
        logger.exception(f"Error fetching job status for {job_id}")
        return JsonResponse({'error': str(e)}, status=500)


@require_POST
def retry_import(request, job_id):
    """
    Create a new job that retries the same CSV file used by job_id.
    Returns new_job_id.
    """
    try:
        # Getting filepath from redis stored location of local directory (media)
        filepath = _r.get(f"job:{job_id}:filepath")
        if not filepath or not os.path.exists(filepath):
            return JsonResponse({'error': 'original file not found'}, status=400)

        # Creating new job id and publishing to redis so UI can poll this
        new_job_id = str(uuid.uuid4())
        _r.set(f"job:{new_job_id}:status", "queued")
        _r.set(f"job:{new_job_id}:progress", 0)
        _r.set(f"job:{new_job_id}:message", "Queued (retry)")
        _r.set(f"job:{new_job_id}:processed", 0)
        _r.set(f"job:{new_job_id}:filepath", filepath)

        # starting processing task as usual
        import_products_task.delay(filepath, new_job_id)
        logger.info(f"Retrying job {job_id} as new job {new_job_id}")
        return JsonResponse({'job_id': new_job_id})
    except Exception as e:
        logger.exception(f"Error retrying job {job_id}")
        return JsonResponse({'error': str(e)}, status=500)


def product_list(request):
    """
    List products with optional filtering and pagination.
    """
    try:
        q = request.GET.get('q', '')
        active = request.GET.get('active', '')
        qs = Product.objects.all()
        if q:
            qs = qs.filter(Q(sku__icontains=q) | Q(name__icontains=q) | Q(description__icontains=q))
        if active.lower() in ['true', 'false']:
            qs = qs.filter(active=(active.lower() == 'true'))

        # pagination logic
        page = int(request.GET.get('page', 1))
        per_page = 25 # currently displaying 25 products per page, can adjust
        total = qs.count()
        start = (page-1)*per_page
        end = start + per_page
        products = qs.order_by('-updated_at')[start:end]
        show_next = total > page * per_page

        return render(request, 'products/list.html', {
            'products': products,
            'page': page,
            'per_page': per_page,
            'total': total,
            'q': q,
            'active': active,
            'show_next': show_next
        })
    except Exception as e:
        logger.exception("Error fetching product list")
        return JsonResponse({'error': str(e)}, status=500)


def product_create(request):
    """
    Create a new product from form submission.
    ToDo: add option to add product to UI
    """
    try:
        if request.method == 'POST':
            form = ProductForm(request.POST)
            if form.is_valid():
                product = form.save()
                # calling webhook if exists for post-create
                _trigger_webhooks('product_created', {'id': product.id, 'name': product.name})
                logger.info(f"Product created: {product.sku}")
                return redirect('products:product_list')
        else:
            form = ProductForm()
        return render(request, 'products/form.html', {'form': form, 'create': True})
    except Exception as e:
        logger.exception("Error creating product")
        return JsonResponse({'error': str(e)}, status=500)


def product_edit(request, pk):
    """
    Edit existing product.
    """
    product = get_object_or_404(Product, pk=pk)
    try:
        if request.method == 'POST':
            form = ProductForm(request.POST, instance=product)
            if form.is_valid():
                form.save()
                # calling webhook if exists for post-update
                _trigger_webhooks('product_updated', {'id': product.id, 'name': product.name})
                logger.info(f"Product updated: {product.sku}")
                return redirect('products:product_list')
        else:
            form = ProductForm(instance=product)
        return render(request, 'products/form.html', {'form': form, 'create': False})
    except Exception as e:
        logger.exception(f"Error editing product {pk}")
        return JsonResponse({'error': str(e)}, status=500)


def product_delete(request, pk):
    """
    Delete a single product.
    """
    product = get_object_or_404(Product, pk=pk)
    try:
        if request.method == 'POST':
            product.delete()
            logger.info(f"Product deleted: {pk}")
            return redirect('products:product_list')
        return render(request, 'products/form.html', {'confirm_delete': True, 'product': product})
    except Exception as e:
        logger.exception(f"Error deleting product {pk}")
        return JsonResponse({'error': str(e)}, status=500)


def product_delete_all(request):
    """
    Delete all products.
    """
    try:
        if request.method == 'POST':
            Product.objects.all().delete()
            logger.info("All products deleted")
            return JsonResponse({'status': 'ok'})
        return JsonResponse({'error': 'method'}, status=400)
    except Exception as e:
        logger.exception("Error deleting all products")
        return JsonResponse({'error': str(e)}, status=500)


def webhooks(request):
    """
    List and create webhooks.
    """
    try:
        if request.method == 'POST':
            form = WebhookForm(request.POST)
            if form.is_valid():
                form.save()
                logger.info("Webhook created")
                return redirect('products:webhooks')
        else:
            form = WebhookForm()
        hooks = Webhook.objects.all()
        return render(request, 'products/webhooks.html', {'form': form, 'hooks': hooks})
    except Exception as e:
        logger.exception("Error handling webhooks page")
        return JsonResponse({'error': str(e)}, status=500)


def webhook_test(request, pk):
    """
    Test a webhook asynchronously.
    """
    try:
        hook = get_object_or_404(Webhook, pk=pk)
        task = test_webhook_task.delay(hook.id)
        logger.info(f"Webhook test queued: {hook.id}")
        return JsonResponse({'task_id': task.id})
    except Exception as e:
        logger.exception(f"Error testing webhook {pk}")
        return JsonResponse({'error': str(e)}, status=500)


def webhook_edit(request, pk):
    """
    Edit a webhook.
    """
    hook = get_object_or_404(Webhook, pk=pk)
    try:
        if request.method == 'POST':
            form = WebhookForm(request.POST, instance=hook)
            if form.is_valid():
                form.save()
                logger.info(f"Webhook edited: {hook.id}")
                return redirect('products:webhooks')
        else:
            form = WebhookForm(instance=hook)
        return render(request, 'products/webhook_form.html', {'form': form, 'edit': True})
    except Exception as e:
        logger.exception(f"Error editing webhook {pk}")
        return JsonResponse({'error': str(e)}, status=500)


@require_POST
def webhook_delete(request, pk):
    """
    Delete a webhook.
    """
    hook = get_object_or_404(Webhook, pk=pk)
    try:
        hook.delete()
        logger.info(f"Webhook deleted: {pk}")
        return redirect('products:webhooks')
    except Exception as e:
        logger.exception(f"Error deleting webhook {pk}")
        return JsonResponse({'error': str(e)}, status=500)
