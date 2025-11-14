from django.urls import path
from . import views

app_name = 'products'

urlpatterns = [
    path('', views.index, name='index'),
    path('upload/', views.upload_file, name='upload_file'),
    path('job-status/<str:job_id>/', views.job_status, name='job_status'),
    path('products/list/', views.product_list, name='product_list'),
    path('products/create/', views.product_create, name='product_create'),
    path('products/edit/<int:pk>/', views.product_edit, name='product_edit'),
    path('products/delete/<int:pk>/', views.product_delete, name='product_delete'),
    path('products/delete_all/', views.product_delete_all, name='product_delete_all'),
    path('webhooks/', views.webhooks, name='webhooks'),
    path('webhooks/test/<int:pk>/', views.webhook_test, name='webhook_test'),
]
