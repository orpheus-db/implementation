import yaml
import logging
# import psycopg2
import sys
import json

from django.db import connection
from django.db.utils import ProgrammingError, IntegrityError, OperationalError
from orpheus.core.orpheus_sqlparse import SQLParser
from orpheus.core.orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError
from orpheus.core.db import UserNotSetError, ConnectionError, OperationError, DatasetExistsError,SQLSyntaxError
from orpheus.core.orpheus_const import DATATABLE_SUFFIX, INDEXTABLE_SUFFIX, VERSIONTABLE_SUFFIX, PUBLIC_SCHEMA

from django.contrib import messages

class DatabaseManager():
    def __init__(self, config, request):
        self.cursor = connection.cursor()
        self.connect = connection
        self.config = config
        self.currentDB = config['database']
        self.user = config['user']
        self.request = request

    def execute_sql(self, sql):
        try:
            print sql
            self.cursor.execute(sql)
            colnames = [desc[0] for desc in self.cursor.description]
            transactions = []
            for row in self.cursor.fetchall():
                transactions.append([str(e) for e in row])
            return colnames, transactions
        except TypeError:
            messages.info(self.request, self.cursor.statusmessage)
            self.connect.commit()
            return None, None
        except ProgrammingError:
            raise SQLSyntaxError()

    def refresh_cursor(self):
        connection.close()
        self.connect = connection
        self.cursor = self.connect.cursor()

    # schema is a list of tuple of (attribute_name, attribute_type) as string
    def create_dataset(self, inputfile, dataset, schema, header=False, attributes=None):
        self.refresh_cursor()
        messages.info(self.request, "Creating dataset %s to %s" % (dataset, self.currentDB))
        # create a schema (in postgres) to store user specific information

        try:
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS %s ;" % self.user)
            self.cursor.execute("CREATE TABLE IF NOT EXISTS %s (dataset_name text primary key);" % (self.user + '.datasets'))
        except ProgrammingError as e:
            # this is ok since table has been created before
            self.refresh_cursor()

        try:
            # add current dataset name into user.datasets
            self.cursor.execute("INSERT INTO %s values('%s');" % (self.user + '.datasets', dataset))
        except IntegrityError: # happens when inserting duplicate key
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
            # TODO: change to private schema in later version
            messages.info(self.request, "Creating datatable using the schema provided")
            # create datatable
            self.cursor.execute("CREATE TABLE %s (rid serial primary key, \
                                                  %s);" % (PUBLIC_SCHEMA + dataset + DATATABLE_SUFFIX, ",".join(map(lambda (attribute_name, attribute_type) : attribute_name + " " + attribute_type, schema))))

            messages.info(self.request, "Creating version table")
            # create version table
            self.cursor.execute("CREATE TABLE %s(vid int primary key, \
                                                 author text, \
                                                 num_records int, \
                                                 parent integer[], \
                                                 children integer[], \
                                                 create_time timestamp, \
                                                 commit_time timestamp, \
                                                 commit_msg text);" % (PUBLIC_SCHEMA + dataset + VERSIONTABLE_SUFFIX))
            messages.info(self.request,   "Creating version table")

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
        except ProgrammingError:
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
            raise BadStateError("config.yaml file not found or data not clean, abort")
            return None
        return obj


