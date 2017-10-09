import psycopg2
from orpheus.core.orpheus_sqlparse import SQLParser
from orpheus.core.orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError
import orpheus.core.orpheus_const as const
from orpheus.core.db import UserNotSetError, ConnectionError, OperationError, DatasetExistsError, SQLSyntaxError


class DatabaseManager():
    def __init__(self, conf):
        # yaml config passed from ctx
        self.config = conf
        try:
            self.db = conf['db']
            self.user = conf['user']
            self.password = conf['password']
            self.connect_str = "host=%s port=%d dbname=%s user=%s password=%s" % (conf['host'], conf['port'], conf['db'], conf['user'], conf['password'])
            self.connect_db()
        except KeyError as e:
            raise BadStateError("context missing field %s, abort" % e.args[0])


    def connect_db(self):
        try:
            self.connect = psycopg2.connect(self.connect_str)
            self.cursor = self.connect.cursor()
        except psycopg2.OperationalError as e:
            # click.echo(e, file=sys.stderr)
            raise ConnectionError("Cannot connect to the database [%s] @ [%s]:[%s]. Check connection, username, password and database name." % (self.config['db'], self.config['host'], self.config['port']))
        return self

    def execute_sql(self, sql):
        try:
            self.cursor.execute(sql)
            if SQLParser.is_select(sql): #return records
                '''
                colnames = [desc[0] for desc in self.cursor.description]
                print ', '.join(colnames)
                for row in self.cursor.fetchall():
                    print ', '.join(str(e) for e in row)
                '''
                return [[str(e) for e in row] for row in self.cursor.fetchall()]
                    
            else:
                return self.cursor.statusmessage
                #print self.cursor.statusmessage
            self.connect.commit() # commit UPDATE/INSERT messages

        except psycopg2.ProgrammingError:
            raise SQLSyntaxError()

    def list_dataset(self):
        #self.refresh_cursor()
        try:
            self.cursor.execute("SELECT * from %s;" % (self.user + '.datasets'))
            return [x[0] for x in self.cursor.fetchall()]
        except psycopg2.ProgrammingError:
            raise BadStateError("No dataset has been initialized before, try init first")
        return

    def refresh_cursor(self):
        self.connect = psycopg2.connect(self.connect_str)
        self.cursor = self.connect.cursor()


    # schema is a list of tuple of (attribute_name, attribute_type) as string
    def create_dataset(self, inputfile, dataset, schema, header=False, attributes=None):
        self.refresh_cursor()
        print "Creating the dataset [%s] to the database [%s] ..." % (dataset, self.db)
        # create a schema (in postgres) to store user specific information
        try:
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS %s ;" % self.user)
            self.cursor.execute("CREATE TABLE IF NOT EXISTS %s (dataset_name text primary key);" % (self.user + '.datasets'))
        except psycopg2.ProgrammingError:
            # this is ok since table has been created before
            self.refresh_cursor()

        try:
            # add current dataset name into user.datasets
            self.cursor.execute("INSERT INTO %s values('%s');" % (self.user + '.datasets', dataset))
        except psycopg2.IntegrityError: # happens when inserting duplicate key
            raise DatasetExistsError(dataset, self.user)
            return

        try:
            # for each dataset, create 3 tables
            # dataset_datatable, which includes all records, rid as PK, based on schema
            # dataset_version, which keep track of all version information, like version
            # dataset_indexTbl, which includes all the vid and rid mapping, like indexTbl


            if '.csv' not in inputfile:
                # TODO: finish other input later
                raise NotImplementedError("Loading other than CSV file not implemented!")
                return

            if not attributes:
                raise NotImplementedError("Attributes inferreing not implemented!")
                return

            # create cvd into public schema
            #TODO: change to private schema in later version

            print "Creating the data table using the schema provided ..."
            # create datatable
            self.cursor.execute("CREATE TABLE %s (rid serial primary key, \
                                                  %s);" % (const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX, ",".join(map(lambda (attribute_name, attribute_type) : attribute_name + " " + attribute_type, schema))))

            print "Creating the version table ..."
            # create version table
            self.cursor.execute("CREATE TABLE %s(vid int primary key, \
                                                 author text, \
                                                 num_records int, \
                                                 parent integer[], \
                                                 children integer[], \
                                                 create_time timestamp, \
                                                 commit_time timestamp, \
                                                 commit_msg text);" % (const.PUBLIC_SCHEMA + dataset + const.VERSIONTABLE_SUFFIX))

            print "Creating the index table ..."
            # create indexTbl table
            self.cursor.execute("CREATE TABLE %s (vid int primary key, \
                                                  rlist integer[]);" % (const.PUBLIC_SCHEMA + dataset + const.INDEXTABLE_SUFFIX))

            # dump data into this dataset
            file_path = self.config['orpheus_home'] + inputfile
            if header:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV HEADER;" % (const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX, ",".join(attributes), file_path))
            else:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV;" % (const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX, ",".join(attributes), file_path))

            self.connect.commit()
        except Exception as e:
            raise OperationError()
        return

    def drop_dataset(self, dataset):
        self.refresh_cursor()
        # TODO: refactor for better approach?
        try:
            self.cursor.execute("DROP table %s;" % (const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()

        try:
            self.cursor.execute("DROP table %s;" % (const.PUBLIC_SCHEMA + dataset + const.VERSIONTABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()

        try:
            self.cursor.execute("DROP table %s;" % (const.PUBLIC_SCHEMA + dataset + const.INDEXTABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()
        try:
            self.cursor.execute("DELETE from %s where dataset_name = '%s';" % (self.user + ".datasets", dataset))
            self.connect.commit()
        except:
            self.refresh_cursor()

        self.connect.commit()
        return

