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
        # create new version
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
        self.conn.cursor.execute(sql)

        # update child column in the parent tuple
        target_parent_vid='{' + ','.join(parent_list) + '}'
        sql = "UPDATE %s SET children = ARRAY_APPEND(children, %s) WHERE vid = ANY('%s' :: int[]);" %(version_graph_name, curt_vid,target_parent_vid)
        self.conn.cursor.execute(sql)
        self.conn.connect.commit()
        return curt_vid

    def update_index_table(self, index_table_name,table_name, parent_table_name,parent_vlist,new_vid,modified_id,new_rids):
        print "update index table"
        # TODO use specific primary key name.

        # get all not changed pk in the cloned table
        modified_id_string = '{'+', '.join(modified_id)+'}'
        sql = "SELECT %s FROM %s WHERE NOT %s = ANY('%s' :: int[]);" %("employee_id", table_name,"employee_id", modified_id_string)
        self.conn.cursor.execute(sql)
        all_not_changed_pk = [t[0] for t in self.conn.cursor.fetchall()]

        # get all rid-pk mapping in the cloned table from source table
        mappingTable = parent_table_name + "_mapTbl"
        rid_list = self.select_records_of_version_list(parent_vlist)
        sql = "SELECT rid, %s INTO %s FROM %s t1 WHERE t1.rid = ANY('%s' :: int[]); " %("employee_id", mappingTable, parent_table_name,rid_list)
        self.conn.cursor.execute(sql)


        # get all not changed rid, this is one part of new version's rid
        all_not_changed_pk_string = '{'+', '.join(map(str,all_not_changed_pk))+'}'
        sql = "SELECT rid FROM %s WHERE %s = ANY('%s' :: int[]);  "%(mappingTable,"employee_id",all_not_changed_pk_string)
        self.conn.cursor.execute(sql)
        all_not_changed_rid = [t[0] for t in self.conn.cursor.fetchall()]
        print 'not changed rids:'
        print all_not_changed_rid

        print "new rids:"
        print new_rids
        all_rids = all_not_changed_rid + new_rids
        sql = 'INSERT INTO %s VALUES (ARRAY[%s], ARRAY[%s])' % (index_table_name, new_vid, all_rids)
        print sql
        self.conn.cursor.execute(sql)

        drop_sql = "DROP TABLE %s" % mappingTable
        self.conn.cursor.execute(drop_sql)
        self.conn.connect.commit()


    def clean(self):
        print "version clean"

    def get_curt_max_vid(self,version_graph_name):
        sql = "SELECT MAX(vid) FROM %s" % version_graph_name
        print sql
        self.conn.cursor.execute(sql)
        result = self.conn.cursor.fetchall()
        print result
        return int(result[0][0])

    def select_records_of_version_list(self, vlist):
        targetv= ','.join(vlist)
        sql = "SELECT distinct rlist FROM indexTbl WHERE vlist && (ARRAY[%s]);"%targetv
        print sql
        self.conn.cursor.execute(sql)
        data = self.conn.cursor.fetchall()
        data_string=''
        for item in data:
            for num in item[0]:
                # print num
                data_string+=str(num)+(',')
        data_string=data_string[0:len(data_string) - 1]
        data_string='{' + data_string+'}'
        # print data_string
        return data_string