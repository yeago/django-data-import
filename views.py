from django.template import RequestContext
from django.shortcuts import render_to_response, get_object_or_404
from django.contrib.contenttypes.models import ContentType
from django_data_import import models as ddi_models

def legacy_data(request,content_type,object_id):
	content_type = get_object_or_404(ContentType,pk=content_type)
	object = get_object_or_404(content_type.model_class(),pk=object_id)
	legacy_data = ddi_models.LegacyData.objects.filter(content_type=content_type,object_id=object_id)
	return render_to_response('ddi/legacy_data_list.html',{'object': object, 'object_list': legacy_data},\
			context_instance=RequestContext(request))
