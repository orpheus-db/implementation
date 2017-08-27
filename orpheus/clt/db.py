import yaml
import logging
import click
import psycopg2
import sys
import json

from orpheus.core.orpheus_sqlparse import SQLParser
from orpheus.core.orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError
from orpheus.core.orpheus_const import DATATABLE_SUFFIX, INDEXTABLE_SUFFIX, VERSIONTABLE_SUFFIX, PUBLIC_SCHEMA
from orpheus.core.db import UserNotSetError, ConnectionError, OperationError, DatasetExistsError,SQLSyntaxError

class DatabaseManager():
    def __init__(self, config):
        # yaml config passed from ctx
        try:
            self.verbose = False
            self.connect = None
            self.cursor = None
            self.config = config
            logging.basicConfig(filename=config['log_path'], format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S ')
            self.user_log = open(config['user_log'], 'a')
            self.home = config['orpheus_home']
            self.currentDB = config['database']
            self.user = config['user']
            self.password = config['passphrase']
            self.connect_str = "host=" + self.config['host'] + " port=" + str(self.config['port']) + " dbname=" + self.currentDB + " user=" + self.user + " password=" + self.password
            self.connect_db()
        except KeyError as e:
            raise BadStateError("context missing field %s, abort" % e.args[0])


    def connect_db(self):
        print "Connecting to DB %s" % self.currentDB
        try:
            if self.verbose:
                click.echo('Trying to connect to %s' % (self.currentDB))
            logging.info('Trying to connect to %s' % (self.currentDB))
            self.connect = psycopg2.connect(self.connect_str)
            self.cursor = self.connect.cursor()
        except psycopg2.OperationalError as e:
            logging.error('%s is not open' % (self.currentDB))
            # click.echo(e, file=sys.stderr)
            raise ConnectionError("Cannot connect to Database %s @ %s:%s. Check connection, username, password and database name." % (self.currentDB, self.config['host'], self.config['port']))
        return self

    def execute_sql(self, sql):
        try:
            self.cursor.execute(sql)
            if SQLParser.is_select(sql): #return records
                colnames = [desc[0] for desc in self.cursor.description]
                print ', '.join(colnames)
                for row in self.cursor.fetchall():
                    print ', '.join(str(e) for e in row)
            else:
                print self.cursor.statusmessage
            self.connect.commit() # commit UPDATE/INSERT messages

        except psycopg2.ProgrammingError:
            raise SQLSyntaxError()

    def refresh_cursor(self):
        self.connect = psycopg2.connect(self.connect_str)
        self.cursor = self.connect.cursor()


    # schema is a list of tuple of (attribute_name, attribute_type) as string
    def create_dataset(self, inputfile, dataset, schema, header=False, attributes=None):
        self.refresh_cursor()
        print "Creating dataset %s to %s" % (dataset, self.currentDB)
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

            print "Creating datatable using the schema provided"
            # create datatable
            self.cursor.execute("CREATE TABLE %s (rid serial primary key, \
                                                  %s);" % (PUBLIC_SCHEMA + dataset + DATATABLE_SUFFIX, ",".join(map(lambda (attribute_name, attribute_type) : attribute_name + " " + attribute_type, schema))))

            print "Creating version table"
            # create version table
            self.cursor.execute("CREATE TABLE %s(vid int primary key, \
                                                 author text, \
                                                 num_records int, \
                                                 parent integer[], \
                                                 children integer[], \
                                                 create_time timestamp, \
                                                 commit_time timestamp, \
                                                 commit_msg text);" % (PUBLIC_SCHEMA + dataset + VERSIONTABLE_SUFFIX))

            print "Creating index table"
            # create indexTbl table
            self.cursor.execute("CREATE TABLE %s (vid int primary key, \
                                                  rlist integer[]);" % (PUBLIC_SCHEMA + dataset + INDEXTABLE_SUFFIX))

            # dump data into this dataset
            file_path = self.config['orpheus_home'] + inputfile

            if header:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV HEADER;" % (PUBLIC_SCHEMA + dataset + DATATABLE_SUFFIX, ",".join(attributes), file_path))
            else:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV;" % (PUBLIC_SCHEMA + dataset + DATATABLE_SUFFIX, ",".join(attributes), file_path))


            self.connect.commit()
        except Exception as e:
            raise OperationError()
        return

    def drop_dataset(self, dataset):
        self.refresh_cursor()
        # TODO: refactor for better approach?
        try:
            self.cursor.execute("DROP table %s;" % (PUBLIC_SCHEMA + dataset + DATATABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()

        try:
            self.cursor.execute("DROP table %s;" % (PUBLIC_SCHEMA + dataset + VERSIONTABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()

        try:
            self.cursor.execute("DROP table %s;" % (PUBLIC_SCHEMA + dataset + INDEXTABLE_SUFFIX))
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

    def list_dataset(self):
        self.refresh_cursor()
        try:
            self.cursor.execute("SELECT * from %s;" % (self.user + '.datasets'))
            return [x[0] for x in self.cursor.fetchall()]
        except psycopg2.ProgrammingError:
            raise BadStateError("No dataset has been initalized before, try init first")
        return

    def show_dataset(self, dataset):
        self.refresh_cursor()
        raise NotImplementedError("Show a specified dataset not implemented!")
        return


    @classmethod
    def load_config(cls):
        try:
            with open('config.yaml', 'r') as f:
                obj = yaml.load(f)
        except IOError:
            raise sys_exception.BadStateError("config.yaml file not found or data not clean, abort")
            return None
        return obj

    @classmethod
    def create_user(cls, user, password, db):
		# Create user in the database
		# Using corresponding SQL or prostegres commands
        # Set one-time only connection to the database to create user
        try:
            server_config = cls.load_config()
            conn_string = "host=" + server_config['host'] + " port=" + str(server_config['port']) + " dbname=" + db
            connect = psycopg2.connect(conn_string)
            cursor = connect.cursor()
            # passphrase = EncryptionTool.passphrase_hash(password)
            cursor.execute("CREATE USER %s SUPERUSER;" % user) # TODO: add password detection later
            connect.commit()
        except psycopg2.OperationalError:
            raise ConnectionError("Cannot connect to %s at %s:%s" % (db, server_config['host'], str(server_config['port'])))
        except Exception as e: # unknown error
            raise e
        return



