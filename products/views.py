import os
import uuid
import csv
import redis
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.conf import settings
from django.db.models import Q
from .forms import UploadFileForm, ProductForm, WebhookForm
from .models import Product, Webhook
from .tasks import import_products_task, test_webhook_task

_r = redis.from_url(settings.REDIS_URL, decode_responses=True)

def index(request):
    form = UploadFileForm()
    return render(request, 'products/index.html', {'form': form})

def upload_file(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            f = form.cleaned_data['file']
            job_id = str(uuid.uuid4())
            upload_dir = os.path.join(settings.MEDIA_ROOT or 'media', 'uploads')
            os.makedirs(upload_dir, exist_ok=True)
            filepath = os.path.join(upload_dir, f"{job_id}.csv")
            with open(filepath, 'wb') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)
            # init redis progress
            _r.set(f"job:{job_id}:status", "queued")
            _r.set(f"job:{job_id}:progress", 0)
            _r.set(f"job:{job_id}:message", "Queued")
            # start celery task
            import_products_task.delay(filepath, job_id)
            return JsonResponse({'job_id': job_id})
    return JsonResponse({'error': 'invalid'}, status=400)

def job_status(request, job_id):
    status = _r.get(f"job:{job_id}:status") or 'unknown'
    progress = int(_r.get(f"job:{job_id}:progress") or 0)
    msg = _r.get(f"job:{job_id}:message") or ''
    return JsonResponse({'status': status, 'progress': progress, 'message': msg})

def product_list(request):
    q = request.GET.get('q', '')
    active = request.GET.get('active', '')
    qs = Product.objects.all()
    if q:
        qs = qs.filter(Q(sku__icontains=q) | Q(name__icontains=q) | Q(description__icontains=q))
    if active.lower() in ['true', 'false']:
        qs = qs.filter(active=(active.lower()=='true'))
    page = int(request.GET.get('page', 1))
    per_page = 25
    total = qs.count()
    start = (page-1)*per_page
    end = start + per_page
    products = qs.order_by('-updated_at')[start:end]
    show_next = total > page * per_page
    return render(request, 'products/list.html', {'products': products, 'page': page, 'per_page': per_page, 'total': total, 'q': q, 'active': active, 'show_next': show_next})

def product_create(request):
    if request.method == 'POST':
        form = ProductForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('products:product_list')
    else:
        form = ProductForm()
    return render(request, 'products/form.html', {'form': form, 'create': True})

def product_edit(request, pk):
    product = get_object_or_404(Product, pk=pk)
    if request.method == 'POST':
        form = ProductForm(request.POST, instance=product)
        if form.is_valid():
            form.save()
            return redirect('products:product_list')
    else:
        form = ProductForm(instance=product)
    return render(request, 'products/form.html', {'form': form, 'create': False})

def product_delete(request, pk):
    product = get_object_or_404(Product, pk=pk)
    if request.method == 'POST':
        product.delete()
        return redirect('products:product_list')
    return render(request, 'products/form.html', {'confirm_delete': True, 'product': product})

def product_delete_all(request):
    if request.method == 'POST':
        Product.objects.all().delete()
        return JsonResponse({'status':'ok'})
    return JsonResponse({'error':'method'}, status=400)

def webhooks(request):
    if request.method == 'POST':
        form = WebhookForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('products:webhooks')
    else:
        form = WebhookForm()
    hooks = Webhook.objects.all()
    return render(request, 'products/webhooks.html', {'form': form, 'hooks': hooks})

def webhook_test(request, pk):
    hook = get_object_or_404(Webhook, pk=pk)
    task = test_webhook_task.delay(hook.id)
    return JsonResponse({'task_id': task.id})
