class VersionManager(object):
    def __init__(self, conn):
    	self.conn = conn;
     	print "init"	

    def create_version_graph(self, table_name):
    	# using CREATE SQL command
    	# table name = graph_name = dataset_name + "version_graph" (or any nicer name..)
    	print "create_version_graph"

    def create_index_table(self, table_name):
    	print "create_index_table"

    def update_version_graph(self, table_name):
    	print "update_version_graph"

    def update_index_table(self, table_name):
    	print "update index table"

    def clean():
    	print "version clean"