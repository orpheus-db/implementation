import yaml
import  logging
import  click
import psycopg2
import sys


class DatabaseManager():
    def __init__(self):
        self.verbose = False
        self.config_path = 'config.yaml'
        self.connect = None
        self.cursor = None
        self.currentDB = None
        self.user = None
        self.password = None
        # TODO still keep the yaml thing?
        with open(self.config_path, 'r') as f:
            self.config = yaml.load(f)
        logging.basicConfig(filename=self.config['log_path'], format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S ')
        self.user_log = open(self.config['user_log'], 'a')
        self.meta_info = self.config['meta_info']
        self.meta_modifiedIds = self.config['meta_modifiedIds']


    def connect_db(self):
        print "connect to DB %s" % self.currentDB
        try:
            if self.verbose:
                click.echo('Trying connect to %s' % (self.config['db']))
            logging.info('Trying to connext to %s' % (self.config['db']))
            conn_string = "host=" + self.config['host'] + " dbname=" + self.currentDB + " user=" + self.user + " password=" + self.password
            self.connect = psycopg2.connect(conn_string)
            self.cursor = self.connect.cursor()
            self.currentDB = self.config['db']
        except psycopg2.OperationalError as e:
            logging.error('%s is not open' % (self.config['db']))
            click.echo(e, file=sys.stderr)
            pass
        return self

    @staticmethod
    def create_user(user, password, db_name):
		# Create user in the database
		# Using corresponding SQL or prostegres commands
		print "add user into database %s" % db_name





