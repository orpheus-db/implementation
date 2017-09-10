
class Print(object):
	def __init__ (self, request = None):
		self.request = request

	def pmessage(self, msg):
		if self.request:
			from django.contrib import messages
			messages.info(self.request, msg)
		else:
			print "%s" % msg

	def perror(self, err):
		if self.request:
			from django.contrib import messages
			messages.error(self.request, err)
		else:
			import click
			click.secho(str(err), fg='red')