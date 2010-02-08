from django.conf.urls.defaults import *

urlpatterns = patterns('django_data_import.views',
	url(r'^ddi/legacy_data/(?P<content_type>\d+)/(?P<object_id>\d+)/$', 'legacy_data',name="ddi_legacy_data"),
)
