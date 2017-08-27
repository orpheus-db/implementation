import os
import yaml
import click

from orpheus.core.executor import Executor
from orpheus.core.user_control import UserManager
from orpheus.core.orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError
from orpheus.core.orpheus_sqlparse import SQLParser
from db import DatabaseManager

class Context():
    def __init__(self):
        self.config_file = 'config.yaml'
        if 'ORPHEUS_HOME' not in os.environ:
            os.environ['ORPHEUS_HOME'] = os.getcwd()
        self.config_path = os.environ['ORPHEUS_HOME'] + '/' + self.config_file
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.load(f)

            assert(self.config['orpheus_home'] != None)

            if not self.config['orpheus_home'].endswith("/"):
                self.config['orpheus_home'] += "/"
            # if user overwrite the ORPHEUS_HOME, rewrite the enviormental parameters
            if 'orpheus_home' in self.config:
                os.environ['ORPHEUS_HOME'] = self.config['orpheus_home']
        except (IOError, KeyError) as e:
            raise BadStateError("config.yaml file not found or data not clean, abort")
            return
        except AssertionError as e:
            raise BadStateError("orpheus_home not specified in config.yaml")
            return
        except: # unknown error
            raise BadStateError("Unknown error during loading the config file, abort")
            return


@click.group()
@click.pass_context
def cli(ctx):
    try:
        ctx.obj = Context().config #Orpheus context obj
        user_obj = UserManager.get_current_state()
        for key in user_obj:
            ctx.obj[key] = user_obj[key]
    except Exception as e:
        click.secho(str(e), fg='red')

@cli.command()
@click.option('--database', prompt='Enter database name', help='Specify the database name that you want to configure to.')
@click.option('--user', prompt='Enter user name', help='Specify the user name that you want to configure to.')
@click.option('--password', prompt=True, hide_input=True, help='Specify the password.', default='')
@click.pass_context
def config(ctx, user, password, database):
    newctx = ctx.obj # default

    try:
        newctx['database'] = database
        newctx['user'] = user
        newctx['passphrase'] = password
        conn = DatabaseManager(newctx)
    except Exception as e:
        click.secho(str(e), fg='red')
        return

    try:
        UserManager.create_user(user, password)
        if UserManager.verify_credential(user, password):
            UserManager.create_user(user, password)
            from encryption import EncryptionTool
            newctx['passphrase'] = EncryptionTool.passphrase_hash(password)
            UserManager.write_current_state(newctx) # pass down to user manager
            click.echo('Logged to database %s as: %s ' % (ctx.obj['database'],ctx.obj['user']))
    except Exception as e:
        click.secho(str(e), fg='red')


@cli.command()
@click.pass_context
def create_user(ctx):
    # check this user has permission to create new user or not
    # create user in UserManager
    if not ctx.obj['user'] or not ctx.obj['database']:
        click.secho("No session in use, please call config first", fg='red')
        return # stop the following commands

    user = click.prompt('Please enter user name')
    password = click.prompt('Please enter password', hide_input=True, confirmation_prompt=True)

    click.echo("Creating user into database %s" % ctx.obj['database'])
    try:
        DatabaseManager.create_user(user, password, ctx.obj['database']) #TODO: need revise
        UserManager.create_user(user, password)
        click.echo('User created.')
    except Exception as e:
        click.secho(str(e), fg='red')

    # TODO: check permission?

@cli.command()
@click.pass_context
def whoami(ctx):
    if not ctx.obj['user'] or not ctx.obj['database']:
        click.secho("No session in use, please call config first", fg='red')
        return # stop the following commands

    click.echo('Logged in database %s as: %s ' % (ctx.obj['database'],ctx.obj['user']))


@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('dataset')
@click.option('--table_name', '-t', help='Create the dataset with existing table schema')
@click.option('--schema', '-s', help='Create the dataset with schema file', type=click.Path(exists=True))
@click.pass_context
def init(ctx, input_file, dataset, table_name, schema):
    # TODO: add header support
    # By default, we connect to the database specified in the -config- command earlier

    # Two cases need to be taken care of:
    # 1.add version control on an outside file
    #    1.1 Load a csv or other format of the file into DB
    #    1.2 Schema
    # 2.add version control on a existing table in DB
    executor = Executor(ctx.obj)
    conn = DatabaseManager(ctx.obj)
    executor.exec_init(input_file, dataset, table_name, schema, conn)

@cli.command()
@click.argument('dataset')
@click.pass_context
def drop(ctx, dataset):
    if click.confirm('Are you sure you want to drop %s?' % dataset):
        try:
            conn = DatabaseManager(ctx.obj)
            click.echo("Dropping dataset %s" % dataset)
            executor = Executor(ctx.obj)
            executor.exec_drop(dataset, conn)
        except Exception as e:
            click.secho(str(e), fg='red')


@cli.command()
@click.option('--dataset', '-d', help='Specify the dataset to show')
@click.option('--table_name', '-t', help='Specify the table to show')
@click.pass_context
def ls(ctx, dataset, table_name):
    # if no dataset specified, show the list of dataset the current user owns
    try:
        conn = DatabaseManager(ctx.obj)
        print "The current database contains the following CVDs:\n"
        if not dataset:
            click.echo("\n".join(conn.list_dataset()))
        else:
            click.echo(conn.show_dataset(dataset))

    # when showing dataset, chop off rid
    except Exception as e:
        click.secho(str(e), fg='red')


# the call back function to execute file
# execute line by line
def execute_sql_file(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    # value is the relative path of file
    conn = DatabaseManager(ctx.obj)
    parser = SQLParser(conn)
    abs_path = ctx.obj['orpheus_home'] + value
    click.echo("Executing SQL file at %s" % value)
    with open(abs_path, 'r') as f:
        for line in f:
            executable_sql = parser.parse(line)
            #print executable_sql
    ctx.exit()

@cli.command()
@click.option('--file', '-f', callback=execute_sql_file, expose_value=False, is_eager=True, type=click.Path(exists=True))
@click.option('--sql', prompt="Input sql statement")
@click.pass_context
def run(ctx, sql):
    try:
        # execute_sql_line(ctx, sql)
        conn = DatabaseManager(ctx.obj)
        parser = SQLParser(conn)
        executable_sql = parser.parse(sql)
        #print executable_sql
        conn.execute_sql(executable_sql)

    except Exception as e:
        import traceback
        traceback.print_exc()
        click.secho(str(e), fg='red')

@cli.command()
@click.argument('dataset')
@click.option('--vlist', '-v', multiple=True, required=True, help='Specify version you want to checkout, use multiple -v for multiple version checkout')
@click.option('--to_table', '-t', help='Specify the table name to checkout to.')
@click.option('--to_file', '-f', help='Specify the location of file')
@click.option('--delimiters', '-d', default=',', help='Specify the delimiter used for checkout file')
@click.option('--header', '-h', is_flag=True, help="If set, the first line of checkout file will be the header")
@click.option('--ignore/--no-ignore', default=False, help='If set, checkout versions into table will ignore duplicated key')
@click.pass_context
def checkout(ctx, dataset, vlist, to_table, to_file, delimiters, header, ignore):
    conn = DatabaseManager(ctx.obj)
    executor = Executor(ctx.obj)
    executor.exec_checkout(dataset, vlist, to_table, to_file, delimiters, header, ignore, conn)


@cli.command()
@click.option('--msg','-m', help='Commit message', required = True)
@click.option('--table_name','-t', help='The table to be committed') # changed to optional later
@click.option('--file_name', '-f', help='The file to be committed', type=click.Path(exists=True))
@click.option('--delimiters', '-d', default=',', help='Specify the delimiters used for checkout file')
@click.option('--header', '-h', is_flag=True, help="If set, the first line of checkout file will be the header")
@click.pass_context
def commit(ctx, msg, table_name, file_name, delimiters, header):

    conn = DatabaseManager(ctx.obj)
    executor = Executor(ctx.obj)
    executor.exec_commit(msg, table_name, file_name, delimiters, header, conn)

@cli.command()
@click.pass_context
def clean(ctx):
    config = ctx.obj
    open(config['meta_info'], 'w').close()
    f = open(config['meta_info'], 'w')
    f.write('{"file_map": {}, "table_map": {}, "table_created_time": {}, "merged_tables": []}')
    f.close()
    click.echo("meta_info cleaned")
    open(config['meta_modifiedIds'], 'w').close()
    f = open(config['meta_modifiedIds'], 'w')
    f.write('{}')
    f.close()
    click.echo("modifiedID cleaned")
