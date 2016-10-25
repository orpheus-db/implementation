class RelationNotExistError(Exception):
  def __init__(self, tablename):
      self.name = tablename
  def __str__(self):
      return "relation %s does not exist" % self.name

class RelationOverwriteError(Exception):
  def __init__(self, tablename):
      self.name = tablename
  def __str__(self):
      return "relation %s exists, add flag to allow overwrite" % self.name

class ReservedRelationError(Exception):
  def __init__(self, tablename):
      self.name = tablename
  def __str__(self):
      return "relation %s is a reserved name, please use a different one" % self.name


class RelationManager(object):
    def __init__(self, conn):
      self.conn = conn;


    def get_datatable_attribute(self, from_table):
      # print 'get attributes now'
      selectTemplate = "SELECT column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '%s' and column_name NOT IN ('rid');" % (from_table)
      # print selectTemplate
      # print self.conn.cursor
      self.conn.cursor.execute(selectTemplate)
      _datatable_attribute_types = self.conn.cursor.fetchall()
      # column name
      _attributes = map(lambda x : str(x[0]), _datatable_attribute_types)
      # data type
      _attributes_type = map(lambda x: str(x[1]), _datatable_attribute_types)
      return _attributes, _attributes_type


    # to_file needs an absolute path
    def checkout(self, vlist, datatable, indextable, to_table=None, to_file=None, delimeters=',', header=False, ignore=False):
        # sanity check
        if to_table:
          if RelationManager.reserve_table_check(to_table):
            raise ReservedRelationError(to_table)
            return
          if self.check_table_exists(to_table): # ask if user want to overwrite
              raise RelationOverwriteError(to_table)
              return
        if not self.check_table_exists(datatable):
            raise RelationNotExistError(datatable)
            return

        _attributes,_attributes_type = self.get_datatable_attribute(datatable)
        recordlist = self.select_records_of_version_list(vlist, indextable)

        if to_table:
          self.checkout_table(_attributes, recordlist, datatable, to_table, ignore)
        if to_file:
          self.checkout_file(_attributes, recordlist, datatable, to_file, delimeters, header)

        self.conn.connect.commit()

    def checkout_file(self, attributes, ridlist, datatable, to_file, delimeters, header):
      # COPY products_273 TO '/tmp/products_199.csv' DELIMITER ',' CSV HEADER;
        sql = "COPY %s (%s) TO '%s' DELIMITER '%s' CSV HEADER;" if header else "COPY %s (%s) TO '%s' DELIMITER '%s' CSV;" 
        sql = sql % (datatable, ','.join(attributes), to_file, delimeters)
        self.conn.cursor.execute(sql)

    # Select the records into a new table
    def checkout_table(self, attributes, ridlist, datatable, to_table, ignore):
        if not ignore:
            sql = "SELECT rid,%s INTO %s FROM %s WHERE rid = ANY('%s'::int[]);" \
              % (', '.join(attributes), to_table, datatable, ridlist)
        else:
            # TODO
            self.get_primary_key(datatable)
            sql = "SELECT rid,%s INTO %s FROM %s WHERE rid = ANY('%s'::int[]);" \
                  % (', '.join(attributes), to_table, datatable, ridlist)
        # print sql
        self.conn.cursor.execute(sql)
        sql = "SELECT %s,rid FROM %s;"%(', '.join(attributes),to_table)
        # print sql
        self.conn.cursor.execute(sql)
        

    def drop_table(self, table_name):
        if not self.check_table_exists(table_name):
            raise RelationNotExistError(table_name)
            return
        drop_sql = "DROP TABLE %s" % table_name
        self.conn.cursor.execute(drop_sql)
        self.conn.connect.commit()


    def select_all_rid(self, table_name):
      select_sql = "SELECT rid from %s;" % table_name
      self.conn.cursor.execute(select_sql)
      return [x[0] for x in self.conn.cursor.fetchall()]

    # return all records that is in table1 not in table2
    def select_complement_table(self, table1, table2, attributes=None):
      if not attributes:
        sql = "TABLE %s EXCEPT TABLE %s;" % (table1, table2)
      else:
        sql = "(SELECT %s from %s) EXCEPT (SELECT %s from %s);" % (','.join(attributes), table1, ','.join(attributes), table2)
      self.conn.cursor.execute(sql)
      return self.conn.cursor.fetchall()

    def select_intersection_table(self, table1, table2, projection='rid'):
      sql = "SELECT %s FROM (SELECT * FROM %s intersect SELECT * FROM %s) as foo;" % (projection, table1, table2)
      self.conn.cursor.execute(sql)
      return self.conn.cursor.fetchall()

    def convert_csv_to_table(self, file_path, destination_table, attributes, delimeters=',', header=False):
      sql = "COPY %s (%s) FROM '%s' DELIMITER '%s' CSV HEADER;" % (destination_table, ",".join(attributes), file_path, delimeters) if header \
          else "COPY %s (%s) FROM '%s' DELIMITER '%s' CSV;" % (destination_table, ",".join(attributes), file_path, delimeters)
      self.conn.cursor.execute(sql)
      self.connect.commit()

    def create_relation(self,table_name):
      # Use CREATE SQL COMMAND
      print "create_relation"

    # will drop existing table to create the new table 
    def create_relation_force(self, table_name, schema):
      if self.check_table_exists(table_name):
        self.drop_table(table_name)
      sql = "CREATE TABLE %s ( like %s including all);" % (table_name, schema)
      self.conn.cursor.execute(sql)
      self.conn.connect.commit()


    def check_table_exists(self,table_name):
      # SQL to check the exisistence of the table
      # print "checking if table %s exists" %(table_name)
      sql= "SELECT EXISTS (" \
           "SELECT 1 " \
           "FROM   information_schema.tables " \
           "WHERE  table_name = '%s');" % table_name
      # print sql
      self.conn.cursor.execute(sql)
      result = self.conn.cursor.fetchall()
      # print result[0][0]
      return result[0][0]

    def update_datatable(self, datatable_name, records):
        print "update_datatable"
        # modified_id_string = '{' + ', '.join(modified_pk) + '}'
        _attributes, _attributes_type = self.get_datatable_attribute(datatable_name)

    #     INSERT INTO films (code, title, did, date_prod, kind) VALUES
    # ('B6717', 'Tampopo', 110, '1985-02-10', 'Comedy'),
    # ('HG120', 'The Dinner Game', 140, DEFAULT, 'Comedy');
        # sql =  "INSERT INTO %s (%s) (SELECT %s FROM %s t1 WHERE t1.%s = ANY('%s' :: int[])) RETURNING rid; " \
        #      %(datatable_name, ', '.join(_attributes), ', '.join(_attributes), table_name, "employee_id", modified_id_string)
        # print sql
        sql = "INSERT INTO %s (%s) VALUES %s RETURNING rid;" % (datatable_name, ', '.join(_attributes), ','.join(records))
        self.conn.cursor.execute(sql)
        new_rids=[t[0] for t in self.conn.cursor.fetchall()]
        self.conn.connect.commit()
        # print new_rids
        return new_rids

    def clean(self):
      print "clean"#????

    @staticmethod
    def reserve_table_check(name):
        '''
        @summary: check if name is reserved
        @param name: name to be checked
        @result: return True if it is reserved
        '''
        # return name == 'datatable' or name == 'indextbl' or name == 'version' or name == 'tmp_table'
        return '_datatable' in name or '_indexTbl' in name or '_version' in name or 'orpheus' in name


    def select_records_of_version_list(self, vlist, indextable):
        targetv= ','.join(vlist)
        print "get rids of version %s" % targetv
        sql = "SELECT distinct rlist FROM %s WHERE vlist && (ARRAY[%s]);" % (indextable, targetv)
        # print sql
        self.conn.cursor.execute(sql)
        data = [','.join(map(str,x[0])) for x in self.conn.cursor.fetchall()]
        data
        return '{' + ','.join(data) + '}'

    def get_primary_key(self,tablename): #this method return nothing, what you want?
        sql="SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type FROM   pg_index i " \
            "JOIN   pg_attribute a ON a.attrelid = i.indrelid " \
            "AND a.attnum = ANY(i.indkey)" \
            "WHERE  i.indrelid = '%s'::regclass " \
            "AND    i.indisprimary;"%tablename
        self.conn.cursor.execute(sql)
        print tablename+'\'s primary key'
        print self.conn.cursor.fetchall()

    def get_number_of_rows(self,tablename):
        sql = "SELECT COUNT (*) from %s" % tablename
        self.conn.cursor.execute(sql)
        result = self.conn.cursor.fetchall()
        # print result
        return result[0][0]