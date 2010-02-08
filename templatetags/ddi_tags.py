from django.template import Library

from django.core.urlresolvers import reverse
from django.contrib.contenttypes.models import ContentType

register = Library()

@register.simple_tag
def legacy_data_url(object):
	return reverse("ddi_legacy_data",args=[\
			ContentType.objects.get_for_model(object.__class__).pk,object.pk])
