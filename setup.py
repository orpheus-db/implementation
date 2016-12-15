from setuptools import setup

setup(
	name='datahub',
	version='1.0',
	# py_modules=['wtfmain', 
	# 			'db', 
	# 			'encryption', 
	# 			'metadata', 
	# 			'orpheus_const', 
	# 			'orpheus_exceptions', 
	# 			'orpheus_sqlparser', 
	# 			'relation', 
	# 			'schema_parser', 
	# 			'user_control', 
	# 			'version',
	# 			'access'],
	py_modules=['click_entry'],
	install_requires=[
		# 'Click', 'psycopg2', 'PyYAML', 'pandas', 'pyparsing'
		'Click'
	],
	entry_points='''
		[console_scripts]
		dh=click_entry:cli
	'''
)
