"""data_center URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from cona.views import ConaView
from common.views import CommonView
from kamba.views import KambaView
from tianjin.views import TianjinView
from customized_chart.views import CustomizedView
urlpatterns = [
    path('admin/', admin.site.urls),
    path("cona/", ConaView.as_view()),
    path("kamba/", KambaView.as_view()),
    path("tianjin/", TianjinView.as_view()),
    path("common/", CommonView.as_view()),
    path("custom/", CustomizedView.as_view()),
]
