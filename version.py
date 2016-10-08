import datetime

class VersionManager(object):
    def __init__(self, conn):
        self.conn = conn;

    def create_version_graph(self, table_name):
        # using CREATE SQL command
        # table name = graph_name = dataset_name + "version_graph" (or any nicer name..)
        print "create_version_graph"

    def create_index_table(self, table_name):
        print "create_index_table"

    def update_version_graph(self, version_graph_name,num_of_records,parent_list,table_create_time,msg):
        print "update_version_graph"
        parent_list_string='\'{'+', '.join(parent_list)+'}\''
        commit_time = str(datetime.datetime.now())
        max_vid = self.get_curt_max_vid(version_graph_name)
        curt_vid = max_vid + 1
        values = "(%s,%s,%s, %s, %s, %s,%s)" % (
        curt_vid,
        num_of_records,parent_list_string,
        "'{}'",
        "'%s'" % table_create_time,
        "'%s'" % commit_time,
        "'%s'" % msg)
        sql = "INSERT INTO %s VALUES %s"% (version_graph_name, values)
        print sql
        # sql = "INSERT INTO version VALUES" % values
        self.conn.cursor.execute(sql)
        # update child column in the parent tuple
        target_parent_vid='{' + ','.join(parent_list) + '}'
        sql = "UPDATE %s SET children = ARRAY_APPEND(children, %s) WHERE vid = ANY('%s' :: int[]);" %(version_graph_name, curt_vid,target_parent_vid)
        self.conn.cursor.execute(sql)
        # print sql
        self.conn.connect.commit()
        return curt_vid

    def update_index_table(self, table_name,vid,new_rids):
        print "update index table"
        sql = 'INSERT INTO %s VALUES (ARRAY[%s], ARRAY[%s])' % (table_name, vid, new_rids)
        self.conn.cursor.execute(sql)


    def clean(self):
        print "version clean"

    def get_curt_max_vid(self,version_graph_name):
        sql = "SELECT MAX(vid) FROM %s" % version_graph_name
        print sql
        self.conn.cursor.execute(sql)
        result = self.conn.cursor.fetchall()
        print result
        return int(result[0][0])