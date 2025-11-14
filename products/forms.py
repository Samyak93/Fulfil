from django import forms
from .models import Product, Webhook

class UploadFileForm(forms.Form):
    file = forms.FileField()

class ProductForm(forms.ModelForm):
    class Meta:
        model = Product
        fields = ['name','sku','description','active']

class WebhookForm(forms.ModelForm):
    class Meta:
        model = Webhook
        fields = ['url','event_type','is_enabled']
