# PostgreSQL schema prefix
PUBLIC_SCHEMA = 'public.'

# CVD suffix
DATATABLE_SUFFIX = '_datatable'
INDEXTABLE_SUFFIX = '_indextable'
VERSIONTABLE_SUFFIX = '_versiontable'
PARTITIONTABLE_SUFFIX = '_partitiontable'

class Constants(object):
	@classmethod
	def getDatatableName(self, dataset, pid):
		return dataset + DATATABLE_SUFFIX + "_p%s" % pid

	@classmethod
	def getIndextableName(self, dataset):
		return dataset + INDEXTABLE_SUFFIX

	@classmethod
	def getVersiontableName(self, dataset):
		return dataset + VERSIONTABLE_SUFFIX

	@classmethod
	def getPartitiontableName(self, dataset):
		return dataset + PARTITIONTABLE_SUFFIX

	@classmethod
	def getPublicSchema(self):
		return PUBLIC_SCHEMA