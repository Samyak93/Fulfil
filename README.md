Acme Product Importer
Author: Samyak Rajesh Shah

A scalable, production-ready web application for importing up to 500,000 products from CSV into a PostgreSQL database with real-time progress tracking, product management UI, bulk delete, and webhook management.

This project uses Django + Celery + Redis + PostgreSQL + Docker Compose.

------------------------------------------------------------
FEATURES

1. CSV Product Import (500k+ records)
- Upload CSV from UI
- Real-time progress updates
- Deduplication by case-insensitive SKU
- Overwrites existing products on conflict
- Asynchronous background processing with Celery

2. Product Management UI
- View, create, update, delete products
- Pagination
- Filtering by SKU, name, description, active status

3. Bulk Delete
- One-click delete all products
- Confirmation popup
- Success & failure indicators

4. Webhook Management
- Add, edit, delete, enable/disable webhooks
- Test trigger with response code and response time
- Async-safe, non-blocking

5. Deployment Ready
- Fully dockerized
- Gunicorn for production server
- Celery workers with autoscale
- PostgreSQL + Redis official images

------------------------------------------------------------
TECH STACK

Backend: Django
Async: Celery
Broker: Redis
Database: PostgreSQL
Deployment: Docker Compose
Server: Gunicorn
ORM: Django ORM

------------------------------------------------------------
PROJECT STRUCTURE
```
Fulfil/
├── Dockerfile
├── docker-compose.yml
├── manage.py
├── requirements.txt
├── acme_product_importer/
│   ├── __init__.py
│   ├── celery.py
│   ├── settings.py
│   ├── urls.py
│   ├── wsgi.py
│   └── asgi.py
├── products/
│   ├── __init__.py
│   ├── admin.py
│   ├── apps.py
│   ├── forms.py
│   ├── models.py
│   ├── tasks.py
│   ├── urls.py
│   ├── views.py
│   ├── templates/
│   │   └── products/
│   │       ├── dashboard.html
│   │       ├── form.html
│   │       ├── list.html
│   │       ├── upload.html
│   │       └── webhooks.html
│   └── migrations/
│       ├── __init__.py
│       └── 0001_initial.py
└── static/
    ├── css/
    └── js/
```
------------------------------------------------------------
RUNNING WITH DOCKER

1. Clone the Repository:
   git clone https://github.com/Samyak93/Fulfil.git
   cd Fulfil

2. Build and Start Services:
   docker-compose up --build

This launches:
- PostgreSQL (5432)
- Redis (6379)
- Django + Gunicorn (8000)
- Celery Worker 1
- Celery Worker 2

------------------------------------------------------------
ACCESS THE APPLICATION

Open in browser:
http://localhost:8000/

------------------------------------------------------------
CSV IMPORT INSTRUCTIONS

1. Go to Upload Products page
2. Upload your CSV (columns required: sku, name, description)
3. Progress bar updates live using Redis polling
4. Results appear once import completes

------------------------------------------------------------
MANUAL MIGRATIONS (Optional)

docker-compose exec web python manage.py migrate

------------------------------------------------------------
LOGGING

Show logs (web & workers):

docker-compose logs -f web
docker-compose logs -f worker1
docker-compose logs -f worker2

------------------------------------------------------------
WEBHOOK TESTING

1. Go to Webhooks Settings
2. Add webhook URL
3. Click "Test"
4. View last response code and timestamp

Testing services you can use:
- https://webhook.site
- https://requestbin.io

------------------------------------------------------------
BULK DELETE

- Click "Delete All Products"
- Confirm deletion
- Celery safely deletes all entries in background

------------------------------------------------------------
STOPPING EVERYTHING

docker-compose down

To also remove PostgreSQL volume:

docker-compose down -v

------------------------------------------------------------
CONTACT

Author: Samyak Rajesh Shah
Email: shahsamyak93@gmail.com
