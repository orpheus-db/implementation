from setuptools import setup


setup(
	name='orpheus',
	version='1.0',
	py_modules=['clt'],
	install_requires=[
		'Click',
	],
	entry_points='''
		[console_scripts]
		dh=main:cli
	'''
)

