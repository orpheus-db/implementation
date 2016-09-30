import yaml
import  logging
import  click
import psycopg2
import sys
import json



class DatabaseManager():
    def __init__(self,userinfo_file):
        self.verbose = False
        self.config_path = 'config.yaml'
        self.connect = None
        self.cursor = None
        self.password = None
        # TODO still keep the yaml thing?
        with open(self.config_path, 'r') as f:
            self.config = yaml.load(f)
        logging.basicConfig(filename=self.config['log_path'], format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S ')
        self.user_log = open(self.config['user_log'], 'a')
        self.meta_info = self.config['meta_info']
        self.meta_modifiedIds = self.config['meta_modifiedIds']

        try:
            with open(userinfo_file, 'r') as f:
                config_info = f.readline()
            config_info = json.loads(config_info)
            db_name = config_info['database']
            user = config_info['user']
            password = config_info['password']
            if not db_name:
                click.echo("Please config user and databse first, use the command config")
                return
        except:
            click.echo("Please config user and database first, use the command config!!!")
            return

        self.currentDB = db_name
        self.user = user
        self.password = password
        self.connect_db()



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





