from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.db.models import get_models, get_model
from django.contrib.contenttypes.generic import GenericForeignKey

from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    def handle(self, label, **options):
        """
        Next lines are more or less straight copy from various places in Django
        including /core/management/commands/dumpdata, where I got this
        """
        try:
            app_label, model_label = label.split('.')

        except ImproperlyConfigured:
            raise CommandError("Unknown application: %s" % app_label)

        model = get_model(app_label, model_label)
        if model is None:
            raise CommandError("Unknown model: %s.%s" % (app_label, model_label))

        items = generate_list(model)
        print "... or type 'RELIST' at any time to regenerate this list"
        while True:
            master = raw_input("Which is the primary / master item?\t:")
            if master == 'RELIST':
                items = generate_list(model)
                continue

            slave = raw_input("Which is the dupe / slave item? <Enter=redo>:")
            if slave == 'RELIST':
                items = generate_list(model)
                continue
            if slave == '':
                continue
            try:
                master = int(master) - 1
                slave = int(slave) - 1
            except ValueError:
                print "You suck. Try again"
                continue

            if master in range(0,len(items)) and slave in range(0,len(items)):
                master_obj = items[master]
                slave_obj = items[slave]
                print "merging %s and %s" % (master_obj,slave_obj)
                merge_model_objects(master_obj,slave_obj)
            else:
                print "Try picking a fucking number from the list this time"
                continue

def generate_list(model):
    count = 1
    list = model.objects.all()
    for i in list:
        print "[%s] %s" % (count, i)
        count += 1
    return list

def merge_model_objects(primary_object, *alias_objects):
    """
    Use this function to merge model objects (i.e. Users, Organizations, Polls, Etc.) 
    and migrate all of the related fields from the alias objects the primary object.
    
    Usage:
    from django.contrib.auth.models import User
    primary_user = User.objects.get(email='good_email@example.com')
    duplicate_user = User.objects.get(email='good_email+duplicate@example.com')
    merge_model_objects(primary_user, duplicate_user)
    """
    # Get a list of all GenericForeignKeys in all models
    # TODO: this is a bit of a hack, since the generics framework should provide a similar
    # method to the ForeignKey field for accessing the generic related fields.
    generic_fields = []
    for model in get_models():
        for field_name, field in filter(lambda x: isinstance(x[1], GenericForeignKey), model.__dict__.iteritems()):
            generic_fields.append(field)
    
    # Loop through all alias objects and migrate their data to the primary object.
    for alias_object in alias_objects:
		# Migrate all foreign key references from alias object to primary object.
		for related_object in alias_object._meta.get_all_related_objects():
			# The variable name on the alias_object model.
			alias_varname = related_object.get_accessor_name()
			# The variable name on the related model.
			obj_varname = related_object.field.name
			related_objects = getattr(alias_object, alias_varname)
			for obj in related_objects.all():
				try:
					setattr(obj, obj_varname, primary_object)
					obj.save()
				except Exception, e:
					print 'While processeng %s, exception: %s' % (obj,str(e))
					while True:
						user_response = raw_input("Do you wish to continue the migration of this object (y/[n])? ")
						if user_response == '' or user_response == 'n':
							raise Exception('User Aborted Merge.')
						elif user_response == 'y':
							break
						else:
							print "Error: you must choose 'y' or 'n'."
					print ""

		# Migrate all many to many references from alias object to primary object.
		for related_many_object in alias_object._meta.get_all_related_many_to_many_objects():
			alias_varname = related_many_object.get_accessor_name()
			obj_varname = related_many_object.field.name
			related_many_objects = getattr(alias_object, alias_varname)
			for obj in related_many_objects.all():
				try:
					getattr(obj, obj_varname).remove(alias_object)
					getattr(obj, obj_varname).add(primary_object)
				except:
					print 'Exception: %s' % str(e)
					while True:
						user_response = raw_input("Do you wish to continue the migration of this object (y/[n])? ")
						if user_response == '' or user_response == 'n':
							raise Exception('User Aborted Merge.')
						elif user_response == 'y':
							break
						else:
							print "Error: you must choose 'y' or 'n'."
					print ""

		# Migrate all generic foreign key references from alias object to primary object.
		for field in generic_fields:
			filter_kwargs = {}
			filter_kwargs[field.fk_field] = alias_object._get_pk_val()
			filter_kwargs[field.ct_field] = field.get_content_type(alias_object)
			for generic_related_object in field.model.objects.filter(**filter_kwargs):
				setattr(generic_related_object, field.name, primary_object)
				generic_related_object.save()
		
		while True:
			user_response = raw_input("Do you wish to keep, delete, or abort the object (%s) %s (k/d/a)? " % (alias_object._get_pk_val(), str(alias_object)))
			if user_response == 'a':
				raise Exception('User Aborted Merge.')
			elif user_response == 'd':
				alias_object.delete()
				break
			elif user_response == 'k':
				break
			else:
				print "Error: you must enter a valid value (k, d, a)."

    return primary_object
