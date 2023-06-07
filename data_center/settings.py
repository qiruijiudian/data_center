"""
Django settings for data_center project.

Generated by 'django-admin startproject' using Django 3.2.6.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""
import platform

from corsheaders.defaults import default_methods
from corsheaders.defaults import default_headers

from pathlib import Path
import os

# 用来区分是否是测试环境
TESTOPTION = True

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-z6cufq0k-@p7(c6&%4o4)l6#me2kd*9x-drh-iwh$ki2_db)q0'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ["*"]


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'corsheaders',
    'rest_framework',
    'cona',
    'kamba',
    'common',
    'tianjin',
    'customized_chart',
    'report'
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

CORS_ALLOW_CREDENTIALS = True   #允许携带cookie
CORS_ORIGIN_ALLOW_ALL = True
CORS_ALLOW_ALL_ORIGINS = True
# CORS_ORIGIN_WHITELIST = ('*')  #跨域增加忽略
CORS_ALLOW_METHODS = default_methods


#允许的请求头
CORS_ALLOW_HEADERS = list(default_headers) + ['XMLHttpRequest', 'X_FILENAME', 'Pragma']

APPEND_SLASH = False
ROOT_URLCONF = 'data_center.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'data_center.wsgi.application'


# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}


# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_URL = '/upload/'

# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# 数据库配置
DATABASE = {
    "Windows": {
        "data": {
            "host": "localhost",
            "user": "root",
            "password": "cdqr2008",
            "database": "data_center_statistical" if TESTOPTION is True else "dc"
        },
        "user": {
            "host": "121.199.48.82",
            "user": "root",
            "password": "cdqr2008",
            "database": "data_center_user"
        },
        "report": {
            "host": "localhost",
            "user": "root",
            "password": "cdqr2008",
            "database": "report"
        }

    },
    "Linux": {
        "data": {
            "host": "121.199.48.82",
            "user": "root",
            "password": "cdqr2008",
            "database": "data_center_statistical" if TESTOPTION is True else "dc"
        },
        "user": {
            "host": "121.199.48.82",
            "user": "root",
            "password": "cdqr2008",
            "database": "data_center_user"
        },
        "report": {
            "host": "localhost",
            "user": "root",
            "password": "cdqr2008",
            "database": "report"
        }

    }
}

DB_CONF = {
    "local": {
        "host": "localhost",
        "user": "root",
        "password": "cdqr2008",
        "database": "report"
    },
    "cloud": {
        "host": "121.199.48.82",
        "user": "root",
        "password": "cdqr2008",
        "database": "report"
    }
}


UPLOAD = os.path.join(BASE_DIR, "file/upload")
DOWNLOAD = os.path.join(BASE_DIR, "file/download")

COLUMN_MAPPING = {
    'available_solar': '太阳能辐射量', 'co2_emission_reduction': 'CO2减排量', 'cop': 'COP', 'cost_saving': '节省电费',
    'heat_collection_efficiency': '太阳能集热效率', 'heat_supply': '供热量',
    'high_temperature_plate_exchange_heat': '高温板换制热量', 'pool_heat_input': '水池输入热量',
    'pool_heat_loss': '水池损失热量', 'pool_heat_output': '水池输出热量', 'power_consumption': '耗电量',
    'solar_collector': '太阳能集热量', 'sum_heat_of_storage': '水池存储热量', 'tower_heat_dissipation': '冷却塔散热量',
    'wshp_heat': '水源热泵制热量'
}

TIME_DATA_INDEX = "time_data"
POINT_NAME = "point_name"
VALUE_NAME = "value"
TPL_PATH = "file/report/templates/kamba_tpl.docx"
REPORT_PATH = "file/report/downloads"
CHART_PATH = "file/report/charts"

TABLE_REPORT = "kamba"

if platform.system() == "Linux":
    TPL_PATH = os.path.join("/home/data_center", TPL_PATH)
    REPORT_PATH = os.path.join("/home/data_center", REPORT_PATH)
    CHART_PATH = os.path.join("/home/data_center", CHART_PATH)

START_DATE = {
    "cona": "2020/12/31 00:00:00",
    "kamba": "2020/08/17 00:00:00",
    "tianjin": "2022/03/15 00:00:00"
}




if TESTOPTION is True:
    DB_NAME = {"kamba": {"common": {"h": "kamba_hours_data", "d": "kamba_days_data"}, "pool": {"h": "kamba_hours_pool_temperature", "d": "kamba_days_pool_temperature"}},
    "cona": {"common": {"h": "cona_hours_data", "d": "cona_days_data"}},
    "tianjin": {"common": "tianjin_commons_data"}
    }
    TIMESTAMP = 'Timestamp'
else:
    DB_NAME = {"kamba": {"common": {"h": "kamba_hours", "d": "kamba_days"}, "pool": {"h": "kamba_pool_hours", "d": "kamba_pool_days"}},
    "cona": {"common": {"h": "cona_hours", "d": "cona_days"}},
    "tianjin": {"common": "tianjin"}
    }
    TIMESTAMP = "time_data"

TIME_DATA_INDEX = "time_data"
POINT_NAME = "point_name"
VALUE_NAME = "value"

DB_ORIGIN = "dc_origin"
DB_DC = "dc"

# 数据库连接
DB_USER = "root"
DB_PASSWORD = "cdqr2008"
# 连接的数据库
DB_HOST = "localhost"
# DB_HOST = "121.199.48.82"
