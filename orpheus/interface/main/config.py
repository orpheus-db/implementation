import os
import yaml
import psycopg2

from orpheus.core.orpheus_exceptions import *
from orpheus.core.orpheus_exceptions import *
from orpheus.core.db import *

CONFIG_FILE = 'config.yaml'
CONFIG = {}


if 'ORPHEUS_HOME' not in os.environ:
    os.environ['ORPHEUS_HOME'] = os.getcwd()
    CONFIG_FILE = os.environ['ORPHEUS_HOME'] + '/' + CONFIG_FILE
else:
    CONFIG_FILE = os.environ['ORPHEUS_HOME'] + CONFIG_FILE
try:
    with open(CONFIG_FILE, 'r') as f:
        CONFIG = yaml.load(f)
    assert(CONFIG['orpheus_home'] != None)

    if not CONFIG['orpheus_home'].endswith("/"):
        CONFIG['orpheus_home'] += "/"
    if 'orpheus_home' in CONFIG:
        os.environ['ORPHEUS_HOME'] = CONFIG['orpheus_home']
except (IOError, KeyError) as e:
    raise BadStateError("config.yaml file not found or data not clean, abort")
except AssertionError as e:
    raise BadStateError("orpheus_home not specified in config.yaml")
except: # unknown error
    raise BadStateError("Unknown error during loading the config file, abort")

# temp solution
CONFIG['database'] = 'sixpluszero'
CONFIG['user'] = 'sixpluszero'
CONFIG['passphrase'] = ''


class DatabaseManager():
    def __init__(self, config):
        # yaml config passed from ctx
        try:
            self.verbose = False
            self.connect = None
            self.cursor = None
            self.config = config
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
            self.connect = psycopg2.connect(self.connect_str)
            self.cursor = self.connect.cursor()
        except psycopg2.OperationalError as e:
            # click.echo(e, file=sys.stderr)
            raise ConnectionError("Cannot connect to Database %s @ %s:%s. Check connection, username, password and database name." % (self.currentDB, self.config['host'], self.config['port']))
        return self

    def sql_records(self, sql):
        # better for small result
        self.cursor.execute(sql)
        return [[str(e) for e in row] for row in self.cursor.fetchall()]
