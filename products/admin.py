from django.contrib import admin
from .models import Product, Webhook

# registering admin models for django-admin
@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ('id','sku','name','active','updated_at')
    search_fields = ('sku','name')

@admin.register(Webhook)
class WebhookAdmin(admin.ModelAdmin):
    list_display = ('url','event_type','is_enabled','last_test_status')
    search_fields = ('url',)
