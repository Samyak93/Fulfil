from django.db import models
from django.db.models import UniqueConstraint
from django.db.models.functions import Lower

class Product(models.Model):
    name = models.CharField(max_length=255)
    sku = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            UniqueConstraint(Lower('sku'), name='unique_sku_ci')
        ]

    def __str__(self):
        return f"{self.sku} — {self.name}"

class Webhook(models.Model):
    EVENT_CHOICES = [
        ('product_imported', 'Product Imported'),
        ('product_created', 'Product Created'),
        ('product_updated', 'Product Updated'),
    ]
    url = models.URLField()
    event_type = models.CharField(max_length=50, choices=EVENT_CHOICES)
    is_enabled = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_tested_at = models.DateTimeField(null=True, blank=True)
    last_test_status = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return f"{self.url} ({self.event_type})"
