from django import forms
from .models import Product, Webhook

# Form for CSV upload
class UploadFileForm(forms.Form):
    file = forms.FileField()

# Form for Product List Display
class ProductForm(forms.ModelForm):
    class Meta:
        model = Product
        fields = ['name','sku','description','active']

# Form for webhooks
class WebhookForm(forms.ModelForm):
    class Meta:
        model = Webhook
        fields = ['url','event_type','is_enabled']
