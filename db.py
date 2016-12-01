import yaml
import logging
import click
import psycopg2
import sys
import json

from encryption import EncryptionTool
from orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError
from orpheus_const import DATATABLE_SUFFIX, INDEXTABLE_SUFFIX, VERSIONTABLE_SUFFIX

# Database Manager exceptions
class UserNotSetError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class OperationError(Exception):
    def __str__(self):
        return 'Operation failure, check system parameters'

class DatasetExistsError(Exception):
    def __init__(self, value, user):
        self.value = value
        self.user = user
    def __str__(self):
        return 'dataset %s exists under user %s' % (self.value, self.user)

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
        print "connect to DB %s" % self.currentDB
        try:
            if self.verbose:
                click.echo('Trying connect to %s' % (self.currentDB))
            logging.info('Trying to connext to %s' % (self.currentDB))
            self.connect = psycopg2.connect(self.connect_str)
            self.cursor = self.connect.cursor()
        except psycopg2.OperationalError as e:
            logging.error('%s is not open' % (self.currentDB))
            # click.echo(e, file=sys.stderr)
            raise ConnectionError("connot connect to %s, check login credential or connection" % self.currentDB)
        return self

    def refresh_cursor(self):
        self.connect = psycopg2.connect(self.connect_str)
        self.cursor = self.connect.cursor()


    # schema is a list of tuple of (attribute_name, attribute_type) as string
    def create_dataset(self, inputfile, dataset, schema, header=False, attributes=None): 
        self.refresh_cursor()
        print "creating dataset %s to %s" % (dataset, self.currentDB)
        # create a schema (in postgres) to store user specific information
        try:
            self.cursor.execute("CREATE SCHEMA %s;" % self.user)
            self.cursor.execute("CREATE TABLE %s (dataset_name text primary key);" % (self.user + '.datasets'))
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


            print "Creating datatable using the schema provided"
            # create datatable
            # self.cursor.execute("CREATE TABLE %s ( like %s including all);" % (dataset + "_datatable", schema))
            # print "CREATE TABLE %s (rid int primary key, \
            #                                       %s);" % (dataset + "_datatable", ",".join(map(lambda (attribute_name, attribute_type) : attribute_name + " " + attribute_type, schema)))
            self.cursor.execute("CREATE TABLE %s (rid serial primary key, \
                                                  %s);" % (dataset + DATATABLE_SUFFIX, ",".join(map(lambda (attribute_name, attribute_type) : attribute_name + " " + attribute_type, schema))))

            print "Creating version table"
            # create version table
            self.cursor.execute("CREATE TABLE %s(vid int primary key, \
                                                 num_records int, \
                                                 parent integer[], \
                                                 children integer[], \
                                                 create_time timestamp, \
                                                 commit_time timestamp, \
                                                 commit_msg text);" % (dataset + VERSIONTABLE_SUFFIX))

            print "Creating index table"
            # create indexTbl table
            self.cursor.execute("CREATE TABLE %s (vlist integer[], \
                                                  rlist integer[]);" % (dataset + INDEXTABLE_SUFFIX))

            # dump data into this dataset
            file_path = self.config['orpheus_home'] + inputfile

            if header:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV HEADER;" % (dataset + DATATABLE_SUFFIX, ",".join(attributes), file_path))
            else:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV;" % (dataset + DATATABLE_SUFFIX, ",".join(attributes), file_path))


            self.connect.commit()
        except Exception as e:
            raise OperationError()
        return

    def drop_dataset(self, dataset):
        self.refresh_cursor()
        # TODO: refactor for better approach?
        try:
            self.cursor.execute("DROP table %s;" % (dataset + DATATABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()
            
        try:
            self.cursor.execute("DROP table %s;" % (dataset + VERSIONTABLE_SUFFIX))
            self.connect.commit()
        except:
            self.refresh_cursor()

        try:
            self.cursor.execute("DROP table %s;" % (dataset + INDEXTABLE_SUFFIX))
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
            passphrase = EncryptionTool.passphrase_hash(password)
            cursor.execute("CREATE USER %s WITH LOGIN ENCRYPTED PASSWORD ' %s ' SUPERUSER;" % (user, passphrase)) # TODO: use different flags

            connect.commit()
        except psycopg2.OperationalError:
            raise ConnectionError("connot connect to %s at %s:%s" % (db, server_config['host'], str(server_config['port'])))
        except Exception as e: # unknown error
            raise e
        return




