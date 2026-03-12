AUTH_USER_MODEL = "api.UserProfile"
"""
Django settings for api_engine project.

Gerado a partir de settings.py.example para rodar localmente.
"""
import os
from datetime import timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SECRET_KEY = "dev-key"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
	"django.contrib.auth",
	"django.contrib.contenttypes",
	"django.contrib.sessions",
	"django.contrib.messages",
	"django.contrib.staticfiles",
	'django.contrib.sites',
	"rest_framework",
	"api",
	"drf_yasg",
	"django_extensions",
	"allauth",
	"allauth.account",
	"allauth.socialaccount",
	"rest_framework.authtoken",
	"rest_auth",
	"rest_auth.registration",
	"corsheaders",
	"rest_framework_simplejwt",
]

MIDDLEWARE = [
	"corsheaders.middleware.CorsMiddleware",
	"django.middleware.security.SecurityMiddleware",
	"django.contrib.sessions.middleware.SessionMiddleware",
	"django.middleware.common.CommonMiddleware",
	"django.middleware.csrf.CsrfViewMiddleware",
	"django.contrib.auth.middleware.AuthenticationMiddleware",
	"django.contrib.messages.middleware.MessageMiddleware",
	"django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "api_engine.urls"
CORS_ORIGIN_ALLOW_ALL = True

TEMPLATES = [
	{
		"BACKEND": "django.template.backends.django.DjangoTemplates",
		"DIRS": [],
		"APP_DIRS": True,
		"OPTIONS": {
			"context_processors": [
				"django.template.context_processors.debug",
				"django.template.context_processors.request",
				"django.contrib.auth.context_processors.auth",
				"django.contrib.messages.context_processors.messages",
			]
		},
	}
]

WSGI_APPLICATION = "api_engine.wsgi.application"

DATABASES = {
	"default": {
		"ENGINE": "django.db.backends.postgresql",
	"NAME": os.environ.get("DB_NAME", "api_engine"),
	"USER": os.environ.get("DB_USER", "postgres"),
	"PASSWORD": os.environ.get("DB_PASSWORD", "123456"),
		"HOST": os.environ.get("DB_HOST", "localhost"),
		"PORT": os.environ.get("DB_PORT", "5432"),
	}
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_L10N = True
USE_TZ = True

STATIC_URL = "/static/"
SITE_ID = 1
