import os
import click
import yaml
import psycopg2
import sys
import logging
import user
import json
import pandas as pd

from db import DatabaseManager, DatasetExistsError
from access import AccessManager
from relation import RelationManager, RelationNotExistError
from version import VersionManager
from metadata import MetadataManager
from user_control import UserManager
from schema_parser import Parser as SchemaParser
from orpheus_sqlparser import parse_select as SQLParser
from orpheus_const import DATATABLE_SUFFIX, INDEXTABLE_SUFFIX, VERSIONTABLE_SUFFIX

from orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError

class Context():
    def __init__(self):
        self.config_path = 'config.yaml'
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.load(f)
        except (IOError, KeyError) as e:
            raise BadStateError("config.yaml file not found or data not clean, abort")
            return
        except: # unknown error
            raise BadStateError("unknown error during loding config file, abort")
            return


@click.group()
@click.pass_context
def cli(ctx):
    try:
        ctx.obj = Context().config
        user_obj = UserManager.get_current_state()
        for key in user_obj:
            ctx.obj[key] = user_obj[key]
        if not ctx.obj or not ctx.obj['user'] or not ctx.obj['database']:
            click.secho("No session in use, please call config first", fg='red')
            return # stop the following commands
    except Exception as e:
        click.secho(str(e), fg='red')

@cli.command()
@click.option('--user', prompt='Enter user name', help='Specify the user name that you want to configure to.')
@click.option('--password', prompt=True, hide_input=True, help='Specify the password.')
@click.pass_context
def config(ctx, user, password):
    newctx = ctx.obj # default
    try:
        if UserManager.verify_credential(user, password):
            from encryption import EncryptionTool
            newctx['user'] = user
            newctx['passphrase'] = EncryptionTool.passphrase_hash(password)
            UserManager.write_current_state(newctx) # pass down to user manager
            click.echo('logged in as %s' % user)
    except Exception as e:
        click.secho(str(e), fg='red')


@cli.command()
@click.option('--user', prompt='Enter user name', help='Specify the user name that you want to create.')
@click.option('--password', prompt=True, hide_input=True, confirmation_prompt=True, help='Specify the password that you want to use.')
@click.pass_context
def create_user(ctx, user, password):
    # check this user has permission to create new user or not
    # create user in UserManager
    click.echo("creating user into %s" % ctx.obj['database'])
    try:
        UserManager.create_user(user, password)
        DatabaseManager.create_user(user, password, ctx.obj['database']) #TODO: need revise
        click.echo('User created.')
    except Exception as e:
        click.secho(str(e), fg='red')

# TODO: check permission?
    print ctx.obj
    
@cli.command()
@click.option('--database', '-d', help='Specify the database that you want to use without changing host and port')
@click.pass_context
def db(ctx, database):
    if database:
        ctx.obj['database'] = database
        UserManager.write_current_state(ctx.obj) # write to persisent store
    click.echo('using: %s' % ctx.obj['database'])

@cli.command()
@click.pass_context
def whoami(ctx):
    click.echo('logged in as: %s' % ctx.obj['user'])

@cli.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('dataset')
@click.option('--table', '-t', help='Create the dataset with exisiting table schema')
@click.option('--schema', '-s', help='Create the dataset with schema file', type=click.Path(exists=True))
@click.pass_context
def init(ctx, input, dataset, table, schema):
    # TODO: add header support
    # By default, we connect to the database specified in the -config- command earlier

    # Two cases need to be taken care of:
    # 1.add version control on an outside file
    #    1.1 Load a csv or other format of the file into DB
    #    1.2 Schema
    # 2.add version control on a existing table in DB

    try:
        conn = DatabaseManager(ctx.obj)
        rel = RelationManager(conn)

        if (not table and not schema) or (table and schema):
            raise BadParametersError("Need either (not both) a table or a schema file")
            return
        abs_path = ctx.obj['orpheus_home'] + schema if schema and schema[0] != '/' else schema

        # the attribute_name should not have rid
        attribute_name , attribute_type = rel.get_datatable_attribute(table) if table else SchemaParser.get_attribute_from_file(abs_path)

    except Exception as e:
        click.secho(str(e), fg='red')
        return

    # at this point, we have a valid conn obj and rel obj
    try:

        # schema of the dataset, of the type (name, type)
        schema_tuple = zip(attribute_name, attribute_type)

        # create new dataset
        conn.create_dataset(input, dataset, schema_tuple, attributes=attribute_name)
        # get all rids in list
        lis_rid = rel.select_all_rid(dataset + DATATABLE_SUFFIX)

        # init version info
        version = VersionManager(conn)
        version.init_version_graph_dataset(dataset, lis_rid)
        version.init_index_table_dataset(dataset, lis_rid)

        click.echo("dataset %s create successful" % dataset)
    except DatasetExistsError as e:
        click.secho(str(e), fg='red')
    except Exception as e:
        # revert back to the state before create
        conn.drop_dataset(dataset)
        click.secho(str(e), fg='red')
    # TODO: What about schema? Automation or specified by user?

@cli.command()
@click.argument('dataset')
@click.pass_context
def drop(ctx, dataset):
    if click.confirm('Are you sure you want to drop %s?' % dataset):
        try:
            conn = DatabaseManager(ctx.obj)
            click.echo("dropping dataset %s" % dataset)
            conn.drop_dataset(dataset)
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
        if not dataset:
            click.echo("\n".join(conn.list_dataset()))
        else:
            click.echo(conn.show_dataset(dataset))

    # when showing dataset, chop off rid
    except Exception as e:
        click.secho(str(e), fg='red')

# execute a single line of sql
# only support select for now
def execute_sql_line(ctx, line):
    try:
        conn = DatabaseManager(ctx.obj)
        relation = RelationManager(conn)
        execution_dict = SQLParser(line) # SELECT only!
    except Exception as e:
        click.secho(str(e), fg='red')
        return

    print execution_dict

    dataset = execution_dict['dataset'][0] if 'dataset' in execution_dict else None
    vlist = execution_dict['versions'] if 'versions' in execution_dict else None
    projection = ",".join(execution_dict['columns'])
    datatable = dataset + DATATABLE_SUFFIX if dataset else None
    indextable = dataset + INDEXTABLE_SUFFIX if dataset else None
    versiontable = dataset + VERSIONTABLE_SUFFIX if dataset else None
    where_expr = execution_dict['where_expr'] if 'where_expr' in execution_dict else None
    try:
        if not dataset:
            # normal sql select
            conn.cursor.execute(line)
            result_tuples = conn.cursor.fetchall()
        else:
            attribute_name, result_tuples = relation.checkout_print(vlist, 
                                                                    datatable, 
                                                                    indextable, 
                                                                    projection=projection, 
                                                                    where=where_expr)

            # print to console
            print "\t".join(attribute_name)
        for tup in result_tuples:
            print "\t".join(map(str, tup))
    except psycopg2.ProgrammingError as e:
        click.secho(str(e.args), fg='red')
        return
    except Exception as e:
        click.secho(str(e), fg='red')
        return


# the call back function to execute file
# execute line by line
def execute_sql_file(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    # value is the relative path of file
    abs_path = ctx.obj['orpheus_home'] + value
    click.echo("Executing SQL file at %s" % value)
    with open(abs_path, 'r') as f:
        for line in f:
            execute_sql_line(line)
    ctx.exit()

@cli.command()
@click.option('--file', '-f', callback=execute_sql_file, expose_value=False, is_eager=True, type=click.Path(exists=True))
@click.option('--sql', prompt="Input sql statement")
@click.pass_context
def run(ctx, sql):
    execute_sql_line(ctx, sql)

@cli.command()
@click.argument('dataset')
@click.option('--vlist', '-v', multiple=True, required=True, help='Specify version you want to checkout, use multiple -v for multiple version checkout')
@click.option('--to_table', '-t', help='Specify the table name to checkout to.')
@click.option('--to_file', '-f', help='Specify the location of file')
@click.option('--delimeters', '-d', default=',', help='Specify the delimeter used for checkout file')
@click.option('--header', '-h', is_flag=True, help="If set, the first line of checkout file will be the header")
@click.option('--ignore', '-i', is_flag=True, help='If set, checkout versions into table will ignore duplicated key')
@click.pass_context
def checkout(ctx, dataset, vlist, to_table, to_file, delimeters, header, ignore):
    # check ctx.obj has permission or not
    if not to_table and not to_file:
        click.secho(str(BadParametersError("Need a destination, either a table (-t) or a file (-f)")), fg='red')
        return

    try:
        conn = DatabaseManager(ctx.obj)
        relation = RelationManager(conn)
    except Exception as e:
        click.secho(str(e), fg='red')
        return

    abs_path = ctx.obj['orpheus_home'] + to_file if to_file and to_file[0] != '/' else to_file

    try:
        metadata = MetadataManager(ctx.obj)
        meta_obj = metadata.load_meta()
        datatable = dataset + DATATABLE_SUFFIX
        indextable = dataset + INDEXTABLE_SUFFIX

        relation.checkout(vlist, datatable, indextable, to_table=to_table, to_file=abs_path, delimeters=delimeters, header=header, ignore=ignore)
        # update meta info
        AccessManager.grant_access(to_table, conn.user)
        metadata.update(to_table, abs_path, dataset, vlist, meta_obj)
        metadata.commit_meta(meta_obj)
        if to_table:
            click.echo("Table %s has been cloned from version %s" % (to_table, ",".join(vlist)))
        if to_file:
            click.echo("File %s has been cloned from version %s" % (to_file, ",".join(vlist)))
    except Exception as e:
        if to_table:
            relation.drop_table(to_table)
        if to_file:
            pass # delete the file
        click.secho(str(e), fg='red')

    

@cli.command()
@click.option('--msg','-m', help='Commit message', required = True)
@click.option('--table_name','-t', help='The table to be committed') # changed to optional later
@click.option('--file_name', '-f', help='The file to be committed', type=click.Path(exists=True))
@click.option('--delimeters', '-d', default=',', help='Specify the delimeter used for checkout file')
@click.option('--header', '-h', is_flag=True, help="If set, the first line of checkout file will be the header")
@click.pass_context
def commit(ctx, msg, table_name, file_name, delimeters, header):

    # sanity check
    if not table_name and not file_name:
        click.secho(str(BadParametersError("Need a source, either a table (-t) or a file (-f)")), fg='red')
        return

    if table_name and file_name:
        click.secho(str(NotImplementedError("Can either commit a file or a table at a time")), fg='red')
        return


    try:
        conn = DatabaseManager(ctx.obj)
        relation = RelationManager(conn)
        metadata = MetadataManager(conn.config)
        version = VersionManager(conn)
    except Exception as e:
        click.secho(str(e), fg='red')
        return
    if table_name and not relation.check_table_exists(table_name):
        click.secho(str(RelationNotExistError(table_name)), fg='red')
        return

    # load parent information about the table
    # We need to get the derivation information of the committed table;
    # Otherwise, in the multitable scenario, we do not know which datatable/version_graph/index_table
    # that we need to update information.
    try:
        abs_path = ctx.obj['orpheus_home'] + file_name if file_name else ctx.obj['orpheus_home']
        parent_vid_list = metadata.load_parent_id(table_name) if table_name else metadata.load_parent_id(abs_path, mapping='file_map')
        click.echo("Parent dataset is %s " % parent_vid_list[0])
        click.echo("Parent versions are %s " % parent_vid_list[1])
    except Exception as e:
        click.secho(str(e), fg='red')
        return
    parent_name = parent_vid_list[0]
    parent_list = parent_vid_list[1]
    datatable_name = parent_name + DATATABLE_SUFFIX
    indextable_name = parent_name + INDEXTABLE_SUFFIX
    graph_name = parent_name + VERSIONTABLE_SUFFIX

    try:
        # convert file into tmp_table first, then set the table_name to tmp_table
        if file_name:
            # need to know the schema for this file
            _attributes, _attributes_type = relation.get_datatable_attribute(datatable_name)

            relation.create_relation_force('tmp_table', datatable_name, sample_table_attributes=_attributes) # create a tmp table
            relation.convert_csv_to_table(abs_path, 'tmp_table', _attributes , delimeters=delimeters, header=header) # push everything from csv to tmp_table
            table_name = 'tmp_table'
    except Exception as e:
        click.secho(str(e), fg='red')
        return


    if table_name:
        try:
            _attributes, _attributes_type = relation.get_datatable_attribute(datatable_name)
            commit_attributes, commit_type = relation.get_datatable_attribute(table_name)
            if len(set(_attributes) - set(commit_attributes)) > 0:
                raise BadStateError("%s and %s have different attributes" % (table_name, parent_name))

            # find the new records
            lis_of_newrecords = relation.select_complement_table(table_name, datatable_name, attributes=_attributes)
            lis_of_newrecords = [map(str, list(x)) for x in lis_of_newrecords]
            lis_of_newrecords = map(lambda x : '(' + ','.join(x) + ')', lis_of_newrecords)

            # find the existing rids
            # by default, assume the first column is the join attribute for inner join
            existing_rids = [t[0] for t in relation.select_intersection_table(table_name, datatable_name, commit_attributes)]

            # insert them into datatable
            new_rids = relation.update_datatable(datatable_name, lis_of_newrecords)
            
            print "Found %s new records" % len(new_rids)
            print "Found %s exisiting records" % len(existing_rids)

            current_version_rid = existing_rids + new_rids
            
            # it can happen that there are duplicate in here
            # num_of_records = relation.get_number_of_rows(table_name)
            table_create_time = metadata.load_table_create_time(table_name) if table_name != 'tmp_table' else None

            # update version graph
            curt_vid = version.update_version_graph(graph_name, len(current_version_rid), parent_list, table_create_time, msg)

            # update index table
            version.update_index_table(indextable_name, curt_vid, current_version_rid)
            print "Commiting version %s with %s records" % (curt_vid, current_version_rid)
        except Exception as e:
            click.secho(str(e), fg='red')
            return

    if relation.check_table_exists('tmp_table'):
        relation.drop_table('tmp_table')


    click.echo("commited")



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
