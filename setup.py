from setuptools import setup

setup(
	name='orpheus',
	version='1.0.2',
	description='OrpheusDB command line tool',
	packages=['orpheus', 'orpheus.clt', 'orpheus.core'],
	url='http://orpheus-db.github.io/',
    # py_modules=['db',
	 		# 	'encryption',
	 		# 	'metadata',
	 		# 	'orpheus_const',
	 		# 	'orpheus_exceptions',
	 		# 	'orpheus_sqlparse',
	 		# 	'relation',
	 		# 	'orpheus_schema_parser',
	 		# 	'user_control',
	 		# 	'version',
	 		# 	'access',
	 		# 	'click_entry'],
	#py_modules=['click_entry'],
	install_requires=[
	    'Click', 'psycopg2', 'PyYAML', 'pandas', 'pyparsing', 'sqlparse', 'django', 'grpcio'
		#'Click'
	],
	license='MIT',
	entry_points='''
		[console_scripts]
		orpheus=orpheus.clt.click_entry:cli
	'''
)
