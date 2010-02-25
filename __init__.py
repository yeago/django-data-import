import pdb
import re
from time import sleep
from decimal import Decimal
from sets import Set

from django.conf import settings

from django.core.paginator import Paginator
from django.db.models import get_model
from django.utils.datastructures import SortedDict
from django.utils.encoding import smart_unicode
from django.db import IntegrityError, reset_queries

from django.db.models.fields import DecimalField 
from django.db.models.fields import CharField 
from django.db.models.fields import TextField 
from django.db.models import ForeignKey 
from django.db.models import OneToOneField 
from django.db.models import ManyToManyField 
from django.db.models.query import QuerySet

from logging import getLogger

import logging

log = getLogger(__name__)
#logging.basicConfig(filename='/tmp/import.log',level=logging.DEBUG)

class ValidationError(Exception):
    "(Almost straight copy from django.forms)"
    def __init__(self, message):
    # ValidationError can be passed any object that can 
    # be printed (usually a string) or a list of objects. "
        if isinstance(message, list):
            self.messages = ([smart_unicode(msg) for msg in message])
        else:
            message = smart_unicode(message)
            self.messages = ([message])

    def __str__(self):
        # This is needed because, without a __str__(), printing an exception
        # instance would result in this:
        # AttributeError: ValidationError instance has no attribute 'args'
        # See http://www.python.org/doc/current/tut/node10.html#handling
        return repr(self.messages)

class ImportBaseClass(object):
    """
    Returns the topmost importation, which will be the last dataset 
    imported since relations need to exist before it can
    """
    def _alpha_importation(self):
        topmost_level = None
        current_level = self 
        while not topmost_level:
            if current_level.parent:
                current_level = current_level.parent
            else:
                topmost_level = current_level

        if not hasattr(topmost_level,'master_variants'):
            topmost_level.master_variants = {}

        return topmost_level

    def _get_variants_for_model(self,model_class):
        key = model_class.__name__

        # Go to first importation and look for master variants of a particular class
        alpha = self._alpha_importation()
        if not key in alpha.master_variants:
            alpha.master_variants[key] = {}

        return alpha.master_variants[key]

class Field(ImportBaseClass):
    " Derived from django.forms.Field "
    creation_counter = 0
    def __init__(self,slave_field_name=None,uses_import=None,prompt=False,\
            max_length=None,max_value=None,if_null=None, value=None,\
            verify_by=None,map=None,clean_function=None):

        if clean_function:
            self.custom_clean_function=clean_function

        if value and prompt:
            raise "Makes no sense to specify 'value' and then ask for prompt"

        self.parent = None
        self.map = map
        self.test = False 
        self.max_length = max_length
        self.prompt = prompt
        self.verify_by = verify_by or []
        self.value = value
        self.slave_field_name = slave_field_name 
        self.max_value = max_value
        self.if_null = if_null
        self.importation = uses_import 

        self.creation_counter = Field.creation_counter
        Field.creation_counter += 1
    
    def prep_work(self,master_field_name,parent):
        " This is basically a second __init__ "
        if not self.parent:
            self.master_field_name = master_field_name
            self.parent = parent
            self.master = self.parent.Meta.master._meta.get_field(master_field_name)
            self.slave = None

            if self.slave_field_name:
                self.slave = self.parent.Meta.slave._meta.get_field(self.slave_field_name)

            if isinstance(self.master,ForeignKey) or isinstance(self.master,OneToOneField):
                if self.importation:
                    unique = False
                    self.importation = self.importation(parent=self)
                    
                    if isinstance(self.master,OneToOneField):
                        unique = True

                    " Recursion ahoy!! "
                    self.importation.do_import(unique=unique,verify_by=self.verify_by) 
                    #self._get_variants_for_model(self.master.rel.to).update(self.importation.dataset)
                elif self.prompt:
                    self._update_get_variants_for_model()

    def _update_get_variants_for_model(self):
        variants = {}
        variants.update(self._get_variants_for_model(self.master.rel.to))
        """
        This runs through the values for all of its possible slave
        values, prompting you to associate each as 'variants' to the master field.
        When this is done the conversion will loop through uninterrupted

        Check the initial import for a master variant dict
        Use whole queryset here because clean_FOO override might need other fields in old QS.
        """

        pdb.set_trace() # Need to copy. Can't be eating th whole QS
        slave_iterable = self.parent.Meta.queryset.order_by(self.slave_field_name) 

        """
        The user didn't provide a custom clean method a la def clean_FOO
        """
        if not hasattr(self.parent, 'clean_%s' % self.master_field_name):
            slave_iterable = slave_iterable.values_list(self.slave_field_name,flat=True).distinct()    

        if self.map:
            for v in slave_iterable:
                if v and not str(v).lower() in variants and \
                        isinstance(self.map[v],self.master.__class__):
                    variants[str(v).lower()] = self.map[v]

        if hasattr(self,'custom_clean_function'):
            for v in slave_iterable:
                if v and not str(v).lower() in variants and \
                        isinstance(self.custom_clean_function(v),self.master.rel.to):
                    variants[str(v).lower()] = self.custom_clean_function(v)

        print "Gathering variants for field '%s' (%s total) | %s" % \
                (self.master_field_name,len(slave_iterable),self.parent)

        
        # May need to redo this so the entire queryset doesn't stay in memory.
        associations = list(self.master.rel.to.objects.all())
        self._show_assoc_options(associations)

        for v in slave_iterable: # Can be QS or values_list depending on above.
            if hasattr(self.parent,'clean_%s' % self.master_field_name):
                v = getattr(self.parent,'clean_%s' % self.master_field_name)(v,None)
            v = str(v).lower()
            if not v in variants:
                variants[v], associations = self._manual_associate(v,associations)

        # Now update the global variants
        self._get_variants_for_model(self.master.rel.to).update(variants)

    def _show_assoc_options(self,associations):
        if not self.test:
            count = 1
            for i in associations:
                print "[%s] %s" % (count, i)
                count += 1

    def _manual_associate(self,slave_value,associations):
        if not self.master.rel.to.objects.count():
            return self._manual_create(slave_value), associations 
            # There's nothing to pick from. Create one!

        while True:
            if self.test and associations:
                return associations[0], associations
            else:    # or let the user pick from the PK list already given.
                input = raw_input("Which above %s to associate with "\
                        "value '%s'? 'LIST' | 'NEW' | <Enter> for None:\t" % \
                        (self.master_field_name,slave_value))

            if input == '':    
                return None, associations
            elif input == 'LIST':
                self._show_assoc_options(associations)
                continue
            elif input == 'NEW':
                new_record = self._manual_create(slave_value)
                associations = list(self.master.rel.to.objects.all())
                return new_record, associations
            try:
                input = int(input) - 1
                if input in range(0,len(associations)):
                    return associations[input], associations
            except ValueError:
                print "You fail. Retry."
    
    def _manual_create(self,value):
        print "Creating new [%s] for old value:\t%s" % (self.master_field_name,value) 
        new_model = {}
        if value == None:
            return None
        while True: #Loop through and have the user assign field values
            for field in self.master.rel.to._meta.fields:
                if not field.name in ['pk','id']:
                    while True:
                        try: #In case their input is no good.
                            input = raw_input("Value for field '%s'? "\
                                    "(value or 'SKIP','RETRY'):\t" % field.name)
                            if input == 'SKIP':
                                return
                            elif input == 'RETRY':
                                return self._manual_create(value)
                            new_model[field.name] = input 
                            break
                        except ValueError:
                            print "You fail. Bad. Please try again"
            try:
                new_object = self.master.rel.to.objects.get_or_create(**new_model)[0]
                print "New object created:\t%s" % new_object 
                return new_object
            except ValueError:
                print "Object failed to create. Please fix these keys and continue:\t%s" % new_model
                pdb.set_trace()
                return self.master.rel.to.objects.get_or_create(**new_model)[0]

    def clean(self,value):
        if self.value != None: 
            """
            Value was explicitly set for this field importer.Field('x',value='FOO') 
            """
            return self.value
    
        if self.map:
            """
            Map was given, a la importer.Field('x',map={'0': False,'1': True}) 
            """
            if str(value) in self.map:
                value = self.map[str(value)]

        if hasattr(self,'custom_clean_function'):
            """
            Route through custom function like imporer.Field('x',custom_function=clean_dates)
            """
            value = self.custom_clean_function(value)

        if isinstance(self.master,ForeignKey) or isinstance(self.master,OneToOneField):
            """
            Some core prep work for FKs. Use the variant already provided by user.
            """
            try:
                return self._get_variants_for_model(self.master.rel.to)[value]
            except KeyError:
                pass
                
        elif isinstance(self.master,DecimalField):
            """
            Respect max_value if field was decimal. TODO: Expand to INT
            """
            if value != None:
                value = Decimal('%s' % value)
                if self.max_value and self.max_value < value:
                    return self.max_value

        elif isinstance(self.master,CharField) or isinstance(self.master,TextField):
            """
            Do some quick and dirty char encoding to make sure source data wasn't junky
            """
            if value:
                try:
                    value = value.encode('ascii','ignore')
                except:
                    return value

                if self.prompt:
                    prompted_value = False
                    while prompted_value == False:
                        raw_value = raw_input("Prompting for %s ('%s'):" % \
                                (self.master_field_name,value)).rstrip()
                        if raw_value == '':
                            prompted_value = None

                        else:
                            prompted_value = raw_value

                    return prompted_value

                if getattr(self.master,'max_length',False) and len(value) > self.master.max_length:
                    return '%s' % value[:self.master.max_length]

        if value is None:
            return self.if_null

        return value

def get_declared_fields(bases, attrs, with_base_fields=True):
    """
    Create a list of form field instances from the passed in 'attrs', plus any
    similar fields on the base classes (in 'bases'). This is used by both the
    Form and ModelForm metclasses.

    If 'with_base_fields' is True, all fields from the bases are used.
    Otherwise, only fields in the 'declared_fields' attribute on the bases are
    used. The distinction is useful in ModelForm subclassing.
    Also integrates any additional media definitions
    """
    fields = []
    for field_name, obj in attrs.items():
        if isinstance(obj,Field):
            attribute = attrs.pop(field_name)
            fields.append((field_name,attribute))

    fields.sort(lambda x, y: cmp(x[1].creation_counter, y[1].creation_counter))

    # If this class is subclassing another Form, add that Form's fields.
    # Note that we loop over the bases in *reverse*. This is necessary in
    # order to preserve the correct order of fields.
    for base in bases[::-1]:
        if hasattr(base, 'fields'):
            fields = base.fields.items() + fields

    if 'Meta' in attrs and hasattr(attrs['Meta'],'map'):
        map_fields = []
        for k, v in attrs['Meta'].map.iteritems():
            map_fields.append((k, Field(v),))


        fields = map_fields + fields

    return SortedDict(fields)

class DeclarativeFieldsMetaclass(type):
    """
    Metaclass that converts Field attributes to a dictionary called
    'fields', taking into account parent class 'fields' as well.
    """
    def __new__(cls, name, bases, attrs):
        attrs['fields'] = get_declared_fields(bases, attrs)
        new_class = super(DeclarativeFieldsMetaclass,cls).__new__(cls, name, bases, attrs)
        return new_class

class BaseImport(ImportBaseClass):
    def __init__(self,override_values=None, queryset=None,\
            verbose=False,slave=False,verify_by=None,\
            parent=None,variants=None):

        try:
            self.Meta.master._meta
        except AttributeError:
            self.Meta.master = get_model(*self.Meta.master.split('.'))

        log.debug("Initializing conversion: %s" % self.__class__.__name__)
        if variants:
            self.master_variants=variants

        self.field_variants = {} 
        self.parent = parent
        if slave:
            self.Meta.slave = slave

        #self.dataset = {}    # Keeps a dict of items created by. {'slave_key' : master_object }
        self.verify_by = verify_by or []
        
        self.verbose = verbose
        self.Meta.queryset = queryset
        self.override_values = override_values or {}
        self.created = 0    # General numbers on the work done in this instance. 

        " Now let's make sure all req'd fields have been specified "
        warnings = []
        for i in self.Meta.master._meta.local_fields:
            if not i.name == "id" and not i.name in [field for field in self.fields] \
                    and not self.Meta.master._meta.get_field(i.name).null:
                warnings.append(i.name)

        if warnings:
            print "Warning! Field(s) [%s] in master model [%s] are not nullable, "\
                    " but you didn't specify them. Ignore if these are taken care of "\
                    " in clean() or have a default" % (",".join(warnings),self.Meta.master._meta.db_table)

        if isinstance(self.Meta.slave,QuerySet):
            if not getattr(self.Meta,'queryset',None):
                self.Meta.queryset = self.Meta.slave
            self.Meta.slave = self.Meta.queryset.model
        else:
            if not getattr(self.Meta,'queryset',None):
                self.Meta.queryset = self.Meta.slave.objects.all()

        for i in self.fields:
            self.fields[i].prep_work(i,self)


        if not self.parent:
            """
            Remove master variant lists which aren't needed
            This should be expanded to clean up before the last conversion
            remove_keys = [i for i in self.master_variants.keys()]
            for name, field in self.fields.iteritems():
                if hasattr(field.master,'rel') and field.master.rel:
                    key = field.master.rel.to.__name__
                    remove_keys = [i for i in remove_keys if not i == key]

            for i in remove_keys:
                print "Deleting master variants for %s" % i
                del self.master_variants[i]

            """
            self.do_import()

    def do_import(self,verify_by=None,unique=False):
        log.debug("Importing %s" % self.__class__.__name__)

        skipped = 0
        no_action = 0 
        """
        We've going to do an additional check for imports which have already been done
        (say two different models link to a User's model).
        """
        check_variants = False
        if self._get_variants_for_model(self.Meta.master):
            check_variants = True 

        """
        Next, we're going to process the legacy records in chunks to make sure
        we don't fill up memory
        """
        paginator = Paginator(self.Meta.queryset,1000)
        for i in range(0,paginator.num_pages):
            reset_queries()
            for slave_record in paginator.page(i + 1).object_list:
                """
                Have we already processed this record in another loop? If so, skip!
                """
                if check_variants and slave_record.pk in self._get_variants_for_model(self.Meta.master):
                    skipped += 1
                    continue

                """
                A sleep here seems to increase performance immensely. Gives db a chance to catch up?
                """
                sleep(.001)
                new_model = None

                """
                If import has already been run and verify_by has been specified, try to
                grab the record quickly
                """
                if verify_by: 
                    new_model = self._soft_import(slave_record,verify_by)

                """
                Regular import method. The long way
                """
                if not new_model:
                    new_model = self._hard_import(slave_record,unique)

                """
                If a record was created, and if this isn't the last phase of the import,
                store the legacy and new pks for quick retrieval later.

                Also, do post_save work if called for by the user
                """
                if new_model: 
                    self._get_variants_for_model(self.Meta.master).update({slave_record.pk: new_model.pk})
                    if hasattr(self,'post_save'):
                        self.post_save(slave_record,new_model)

                    if getattr(settings,'DDR_LEGACY_SALVAGE',False):
                        from django.contrib.contenttypes.models import ContentType
                        from django_data_import import models as ddrmodels 
                        for i in self.Meta.slave._meta.get_all_field_names():
                            if getattr(slave_record,i):
                                ddrmodels.LegacyData.objects.get_or_create(object_id = new_model.pk,content_type=\
                                    ContentType.objects.get_for_model(self.Meta.master),legacy_field=i,legacy_value=getattr(slave_record,i))

                else:
                    no_action += 1

        print "[%s] Processed: %s, Created: %s, Skipped: %s, No action: %s" % (self.Meta.master,self.Meta.queryset.count(),self.created, skipped, no_action)
        log.debug("Processed: %s, Created: %s" % (self.Meta.queryset.count(),self.created))
        self.created = 0 

    """
    Used to match a table only by one field
    Obviously this *NEVER* fires unless imported data exists.

    This gives the user a quick way to grab models already imported
    """
    def _soft_import(self,slave_record,verify_by):

        """
        They may pass in a tuple which gives the correspondence.

        If the field names are the same in master/slave, just pass one
        """
        m_field = verify_by[0]
        try:
            s_field = verify_by[1] # Tuple, (m,s)
            raise
        except:
            s_field = m_field # One field name for both 
    
        verify_kwarg = {m_field: getattr(slave_record,s_field)}
        try:
            return self.Meta.master.objects.get(**verify_kwarg)
        except self.Meta.master.DoesNotExist:
            return

    def _hard_import(self,slave_record,unique):
        """
        Regular import method. Goes the long way to construct a record from
        legacy data
        """
        cleaned_data = {}
        for field_name, field in self.fields.iteritems():
            value = None

            """
            We don't assume there was any slave field.
            """
            if field.slave_field_name:
                value = getattr(slave_record,field.slave_field_name)

            if field.importation:
                """ 
                To conserve memory, related importations (uses_import=OtherImport) store
                only the pk created. But from here on out we need the object.
                """
                if not value:
                    value = slave_record.pk
                if value in field._get_variants_for_model(field.master.rel.to):
                    value = field.master.rel.to.objects.get(pk=field._get_variants_for_model(field.master.rel.to)[value])
                else:
                    """
                    User needs to take care of it in clean()
                    """
                    pass
                    value = None # hackfix to a bug. tracking...

                log.debug("Grabbing related, ended up with %s" % value)

            """
            Maybe a value was hard-passed into the field. Just use it.

            Otherwise, pass it through the built-in quick and dirty clean methods
            """
            if field_name in self.override_values and value in self.override_values[field_name]:
                value = self.override_values[field_name][value]
            else:
                value = self.fields[field_name].clean(value)

            cleaned_data[field_name] = value

            """
            If the user specified a custom clean_FOO, call it now
            """
            if hasattr(self, 'clean_%s' % field_name): 
                cleaned_data[field_name] = getattr(self,'clean_%s' % field_name)(slave_record,cleaned_data)

        """
        If they specified a general clean function, call it now
        """
        if hasattr(self,'clean'):
            cleaned_data = self.clean(slave_record,cleaned_data)

        """
        If we didn't end up with data (probably return None in the clean() method),
        let's not try to import anything. No record created. Otherwise, import!
        """
        if cleaned_data:
            return self._hard_import_commit(cleaned_data,unique)


    def _hard_import_commit(self,cleaned_data,unique,tries=0):
        if tries > 5:
            pdb.set_trace()

        try:
            new_model, created = self.Meta.master.objects.get_or_create(**cleaned_data)
            if created:
                self.created += 1
                log.debug("Created %s" % new_model)
            return new_model

        except self.Meta.master.MultipleObjectsReturned:
            if tries > 5:
                pdb.set_trace()
            """
            When re-running, we want to avoid re-creating records.
            However, here, two imported models turned out identical. 
            Because other records may OneToOne to this one, we must
            somehow respect the existance of two identical models 
            and reliably match up OneToOnes after all is said and done.

            For now we use the first of a QuerySet of those models, 
            excluding those that have already been used!

            If other models OneToOne with this one, you will need
            to script around the fact that many records of different
            models will end up pointing to this one.
            """
            if not unique:
                raise self.Meta.master.MultipleObjectsReturned

            # Excluding records already created this run, we return the next in line. Get the next unclaimed copy
            all_dupes = self.Meta.master.objects.filter(**cleaned_data).values_list('pk',flat=True).distinct()
            unprocessed_ids = Set(all_dupes) - Set(self._get_variants_for_model(self.Meta.master).values())
            if unprocessed_ids:
                if tries > 5:
                    pdb.set_trace()
                unprocessed_id = unprocessed_ids.pop()
                log.debug("Multiple objects returned, grabbing latest unprocessed")
                return self.Meta.master.objects.get(pk=unprocessed_id)

            if tries > 5:
                pdb.set_trace()
            log.debug("Multiple objects returned; created a new one")
            return self.Meta.master.objects.create(**cleaned_data)
    
        except IntegrityError, e: 
            if tries > 5:
                pdb.set_trace()
            # Hate this next line. Probably MySQL only. Sorry!
            try:
                match = re.search("Duplicate entry '(\d+)' for key (\d+)",e.args[1])
                errant_pk, key = int(match.groups(0)[0]), int(match.groups(0)[1])
            except:
                pdb.runcall(self._escape,cleaned_data,e)

            log.debug("Duplicate entry error/OneToOneField respect phase. Exception: %s" % e)
            # While lots of things can cause dupes, we're only here to fix OneToOne
            one_to_one_fields = [f for f in self.Meta.master._meta.fields \
                    if getattr(f,'unique')]

            errant_field = one_to_one_fields[key - 1]

            cleaned_data_nodupes = cleaned_data.copy()

            del cleaned_data_nodupes[errant_field.verbose_name]
            # First, let's see if this model has already been imported and has its own correct errant model
            all_dupes = self.Meta.master.objects.filter(**cleaned_data).values_list('pk',flat=True).distinct()
            unprocessed_ids = Set(all_dupes) - Set(self._get_variants_for_model(self.Meta.master).values())
            if unprocessed_ids:
                if tries > 5:
                    pdb.set_trace() # Loop. Damn.
                unprocessed_id = unprocessed_ids.pop()
                log.debug("Got a OneToOne keyerror but just grabbed the latest unclaimed copy")
                correct_dupe = self.Meta.master.objects.filter(pk=unprocessed_id).get(**cleaned_data_nodupes)
                cleaned_data[f.verbose_name] = getattr(correct_dupe,f.verbose_name)
                return self._hard_import_commit(cleaned_data,unique,tries=tries+1)

            log.debug("Got a OneToOne keyerror, but created one anew")
            """
            We need a new copy of the errant model, but if the model has a base model class
            then this method won't work.

            This also fails to work if a unique key is dynamically generated in the errant model's import

            hrmz....
            """
            cleaned_data[errant_field.verbose_name].pk = None
            cleaned_data[errant_field.verbose_name].save()

            if tries > 5:
                pdb.set_trace()

            # Recurse. Maybe there's other issues to solve.
            return self._hard_import_commit(cleaned_data,unique,tries=tries+1)

        except Exception, e:
            pdb.runcall(self._escape,cleaned_data,e)

    def _escape(self,cleaned_data,e):
        print '==ERR0R=ADDING=MODEL=======(Probably your fault)==========='
        print e
        print '==Data that failed:'
        print '%s' % cleaned_data
        print '===========You\'re in pdb! Find out why!================='
    
class Import(BaseImport):
    __metaclass__ = DeclarativeFieldsMetaclass
