import yaml
import logging
import click
import psycopg2
import sys
import json


import exceptions as sys_exception

# Database Manager exceptions
class UserNotSetError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class DatabaseManager():
    def __init__(self, config):
        self.verbose = False
        self.config_path = 'config.yaml'
        # self.current_config = '.meta/config' pass from ctx
        self.connect = None
        self.cursor = None
        self.password = None
        # TODO still keep the yaml thing? Yes.
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.load(f)
            
            logging.basicConfig(filename=self.config['log_path'], format='%(asctime)s %(message)s',
                                datefmt='%m/%d/%Y %I:%M:%S ')
            self.user_log = open(self.config['user_log'], 'a')
            self.meta_info = self.config['meta_info']
            self.meta_modifiedIds = self.config['meta_modifiedIds']
        except (IOError, KeyError):
            raise sys_exception.BadStateError("config.yaml file not found or data not clean, abort")
            return
        except: # unknown error
            raise sys_exception.BadStateError("unknown error during loding config file, abort")
            return

        self.currentDB = config['database']
        self.user = config['user']
        self.password = config['passphrase']
        self.connect_db()



    def connect_db(self):
        print "connect to DB %s" % self.currentDB
        try:
            if self.verbose:
                click.echo('Trying connect to %s' % (self.config['db']))
            logging.info('Trying to connext to %s' % (self.config['db']))
            conn_string = "host=" + self.config['host'] + " port=" + str(self.config['port']) + " dbname=" + self.currentDB + " user=" + self.user + " password=" + self.password
            self.connect = psycopg2.connect(conn_string)
            self.cursor = self.connect.cursor()
            self.currentDB = self.config['db']
        except psycopg2.OperationalError as e:
            logging.error('%s is not open' % (self.config['db']))
            click.echo(e, file=sys.stderr)
            raise ConnectionError("connot connect to %s, check login credential or connection" % self.config['db'])
            
        return self

    @staticmethod
    def create_user(user, password, db_name):
		# Create user in the database
		# Using corresponding SQL or prostegres commands
		print "add user into database %s" % db_name





