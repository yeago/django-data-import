from datetime import datetime
from time import strptime

def get_variant(Model,dict,name):
	for i in Model.objects.order_by('name'):
		print "[%s] - %s" % (i.id,i.name)
	print "Which of the above shall we associate with '%s'\
			(mistype or 0 will create new)?" % name 
	return Model.objects.get(pk=raw_input())
