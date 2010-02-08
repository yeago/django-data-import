from django.db import models
from django.contrib.contenttypes import generic

class LegacyData(models.Model):
	legacy_field = models.CharField(max_length=50)
	legacy_value = models.TextField()
	object_id = models.PositiveIntegerField()
	content_type = models.ForeignKey('contenttypes.ContentType')
	content_object = generic.GenericForeignKey()

	def __unicode__(self):
		return '%s data for %s' % (self.key,self.content_object)
