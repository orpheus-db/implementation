class RelationManager(object):
    def __init__(self, conn):
      self.conn = conn;


    def __get_datatable_attribute(self):
      conn.cursor.execute(connector.gen.simple_select_datatable_attribute_template)
      _datatable_attribute_types = connector.cursor.fetchall()
      _attributes = map(lambda x : str(x[0]), _datatable_attribute_types)
      return _attributes, _attributes_type

    # Select the records into a new table
    def checkout_table(self,from_table, to_table, vlist):
      _attributes = self.__get_datatable_attribute()
      sql = "SELECT %s into %s FROM %s WHERE ARRAY[%s] <@ vlist" % (_attributes, from_table, to_table, vlist);
      print "sql: %s" % sql
      conn.cursor.execute(sql);

    def create_relation(self,table_name):
      # Use CREATE SQL COMMAND
      print "create_relation"

    def check_table_exists(self,table_name):
      # SQL to check the exisistence of the table
      print "check_table_exists";
      return True;

    def update_datatable(self, table_name):
      print "update_datatable";

    def clean(self):
      print "clean"
    
