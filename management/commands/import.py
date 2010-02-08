from django.core.management.base import BaseCommand
from optparse import make_option

class Command(BaseCommand):
	args = ['conversion class...']
	"""
	option_list = BaseCommand.option_list + (
			make_option('--debug', default='False', dest='debug',
				help='Specifies the output serialization format for fixtures.'),
			)
	"""

	def handle(self, conversion_class, **options):
		"""
		keep_log = options.pop('log',None)
		if keep_log:
			logfile = options.pop('logfile','/tmp/%s.import.log' % conversion_class)
			from logging import logger
			logging.basicConfig(level=logging.DEBUG,filename=logfile)
			logging.getLogger('django_data_import').setLevel(logging.DEBUG)
		"""

		conversion = __import__('conversion.imports')
		getattr(getattr(conversion,'imports'),conversion_class)()
