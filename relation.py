class RelationManager(object):
    def __init__(self, conn):
      self.conn = conn;


    def __get_datatable_attribute(self, from_table):
      print 'try to get attributes now'
      selectTemplate = "SELECT column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '%s' and column_name NOT IN ('rid');" % (from_table)
      # print selectTemplate
      # print self.conn.cursor
      self.conn.cursor.execute(selectTemplate)
      _datatable_attribute_types = self.conn.cursor.fetchall()
      # column name
      _attributes = map(lambda x : str(x[0]), _datatable_attribute_types)
      print _attributes
      # data type
      _attributes_type = map(lambda x: str(x[1]), _datatable_attribute_types)
      print  _attributes_type
      return _attributes, _attributes_type

    # Select the records into a new table
    def checkout_table(self,vlist, from_table, to_table,ignore):
        print 'try to check out now'
        if self.check_table_exists(to_table):
            error_msg= "relation "+ to_table + " already exists."
            raise ValueError(error_msg)
            return
        if not self.check_table_exists(from_table):
            error_msg= "relation "+from_table+" does not exist."
            raise ValueError(error_msg)
            return

        _attributes,_attributes_type = self.__get_datatable_attribute(from_table)

        recordlist = self.select_records_of_version_list(vlist)
        if not ignore:
            sql = "SELECT %s,rid INTO %s FROM %s WHERE rid = ANY('%s'::int[]);" \
              % (', '.join(_attributes), to_table, from_table,recordlist)
        if ignore:
            # TODO
            self.get_primary_key(from_table)
            sql = "SELECT %s,rid INTO %s FROM %s WHERE rid = ANY('%s'::int[]);" \
                  % (', '.join(_attributes), to_table, from_table, recordlist)
        print sql
        self.conn.cursor.execute(sql)
        sql = "SELECT %s,rid FROM %s;"%(', '.join(_attributes),to_table)
        print sql
        self.conn.cursor.execute(sql)
        print self.conn.cursor.fetchall()
        self.conn.connect.commit()


    def create_relation(self,table_name):
      # Use CREATE SQL COMMAND
      print "create_relation"

    def check_table_exists(self,table_name):
      # SQL to check the exisistence of the table
      print "check_table_exists"
      sql= "SELECT EXISTS (" \
           "SELECT 1 " \
           "FROM   information_schema.tables " \
           "WHERE  table_name = '%s');" % table_name
      print sql
      self.conn.cursor.execute(sql)
      result = self.conn.cursor.fetchall()
      print result[0][0]
      return result[0][0]

    def update_datatable(self, table_name):
      print "update_datatable";

    def clean(self):
      print "clean"

    @staticmethod
    def reserve_table_check(name):
        '''
        @summary: check if name is reserved
        @param name: name to be checked
        @result: return True if it is reserved
        '''
        # return name == 'datatable' or name == 'indextbl' or name == 'version' or name == 'tmp_table'
        return name == 'datatable' or name == 'indextbl' or name == 'version'


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

    def get_primary_key(self,tablename):
        sql="SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type FROM   pg_index i " \
            "JOIN   pg_attribute a ON a.attrelid = i.indrelid " \
            "AND a.attnum = ANY(i.indkey)" \
            "WHERE  i.indrelid = '%s'::regclass " \
            "AND    i.indisprimary;"%tablename
        self.conn.cursor.execute(sql)
        print tablename+'\'s primary key!!'
        print self.conn.cursor.fetchall()