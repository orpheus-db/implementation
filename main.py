import os
import click
import yaml
import psycopg2
import sys
import logging
import user
import json
import pandas as pd

from db import DatabaseManager
from access import AccessManager
from relation import RelationManager
from version import VersionManager
from metadata import MetadataManager
from user_control import UserManager

@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = UserManager.get_current_state()
    if not ctx.obj or not ctx.obj['user'] or not ctx.obj['database'] or not ctx.obj['passphrase']:
        click.secho("No session in use, please call config first", fg='red')

@cli.command()
@click.option('--user', prompt='Enter user name', help='Specify the user name that you want to configure to.')
@click.option('--password', prompt=True, hide_input=True, help='Specify the password.')
@click.pass_context
def config(ctx, user, password):
    newctx = ctx.obj or {"user": None, "passphrase" : None, "database": "demo"} # default
    try:
        if UserManager.verify_credential(user, password):
            from encryption import EncryptionTool
            newctx['user'] = user
            newctx['passphrase'] = EncryptionTool.passphrase_hash(password)
            UserManager.write_current_state(newctx) # pass down to user manager
    except Exception as e:
        click.secho(str(e), fg='red')


@cli.command()
@click.option('--user', prompt='Enter user name', help='Specify the user name that you want to create.')
@click.option('--password', prompt=True, hide_input=True, confirmation_prompt=True, help='Specify the password that you want to use.')
@click.pass_context
def create_user(ctx, user, password):
    # check this user has permission to create new user or not
    # create user in UserManager
    try:
        UserManager.create_user(user, password)
        # DatabaseManager.create_user(user, passphrase, database) # TODO
        click.echo('User created.')
    except Exception as e:
        click.secho(str(e), fg='red')
    
@cli.command()
@click.option('--database', '-d', help='Specify the database that you want to use')
@click.pass_context
def db(ctx, database):
    # TODO: check permission?
    if database:
        ctx.obj['database'] = database
        UserManager.write_current_state(ctx.obj) # write to persisent store
    click.echo('using: %s' % ctx.obj['database'])

@cli.command()
@click.pass_context
def whoami(ctx):
    click.echo('logged in as: %s' % ctx.obj['user'])

@cli.command()
@click.option('--dataset', '-n', required=True, help='Specify the dataset what you want to init into the DB')
@click.pass_context
def init_dataset(ctx, dataset):
    # By default, we connect to the database specified in the -config- command earlier

    # Two cases need to be taken care of:
    # 1.add version control on an outside file
    #    1.1 Load a csv or other format of the file into DB
    #    1.2 Schema
    # 2.add version control on a existing table in DB
    conn = DatabaseManager(ctx.obj)

    version = VersionManager(conn)
    version.create_version_graph(dataset)
    version.create_index_table(dataset)

    relation = RelationManager(conn)
    relation.create_relation(dataset)
    # TODO: What about schema? Automation or specified by user?

@cli.command()
@click.option('--vlist', '-v', multiple=True, required=True, help='Specify version you want to clone, use multiple -v for multiple version checkout')
@click.option('--from_table', '-f', required=True, help='Specify the table name to checkout from')
@click.option('--to_table', '-t', required=True, help='Specify the table name to checkout to.')
@click.option('--ignore', '-i', help='If set, clone versions into table will ignore duplicated key', is_flag=True)
@click.pass_context
def clone(ctx, vlist, from_table, to_table, ignore):
    # check ctx.obj has permission or not
    try:
        # connect to db
        conn = DatabaseManager(ctx.obj)
        relation = RelationManager(conn)
        relation.checkout_table(vlist, from_table, to_table, ignore)
        # update meta info
        AccessManager.grant_access(to_table, conn.user)
        metadata = MetadataManager(conn)
        metadata.update(to_table,from_table,vlist)
        click.echo("Table %s has been cloned from version %s" % (to_table, ",".join(vlist)))
    except Exception as e:
        click.secho(str(e), fg='red')

    

@cli.command()
@click.option('--msg','-m', help='Commit message', required = True)
@click.option('--table_name','-t', help='The table to be committed', required = True)
@click.pass_context
def commit(ctx, msg, table_name):

    conn = DatabaseManager(ctx.obj)
    relation = RelationManager(conn)
    metadata = MetadataManager(conn)

    if not relation.check_table_exists(table_name):
        click.echo("Table %s not found, abort" % table_name)
        return

    # load parent information about the table
    # We need to get the derivation information of the committed table;
    # Otherwise, in the multitable scenario, we do not know which datatable/version_graph/index_table
    # that we need to update information.
    try:
        parent_vid_list = metadata.load_parent_id(table_name)
        click.echo("Parent table is %s " % parent_vid_list[0])
        click.echo("Parent versions are %s " % parent_vid_list[1])
    except ValueError as err:
        print(err.args)
        return
    parent_name = parent_vid_list[0]
    parent_list = parent_vid_list[1]


    # update corresponding version graph
    version = VersionManager(conn)
    try:
        graph_name = parent_name + "_version_graph"
        num_of_records = relation.get_number_of_rows(table_name)
        table_create_time = metadata.load_table_create_time(table_name)
        # TODO use real version graph name
        curt_vid = version.update_version_graph("version",num_of_records,parent_list,table_create_time,msg)
    except:
        print "update version graph error"
        return

    # load changes to the table
    try:
        modified_id = metadata.load_modified_id(table_name)
    except ValueError as err:
        print(err.args)
        return
    modified_id = map(str, modified_id)

    # append new records to the datatable
    try:
        new_rids = relation.update_datatable(parent_name,table_name,modified_id)
    except:
        print "update datatable error"
        return

    # update index table
    try:
        # TODO use real index name
        version.update_index_table("indexTbl",table_name,parent_name,parent_list,curt_vid,modified_id,new_rids)
    except:
        print "update index table error"
        return



    # TODO: Before return, we may also need to clean table if any.

    click.echo("commited")



@cli.command()
@click.pass_context
def clean(ctx):
    conn = DatabaseManager(ctx.obj)
    open(conn.meta_info, 'w').close()
    f = open(conn.meta_info, 'w')
    f.write('{"file_map": {}, "table_map": {}, "table_created_time": {}, "merged_tables": []}')
    f.close()
    click.echo("meta_info cleaned")
    open(conn.meta_modifiedIds, 'w').close()
    f = open(conn.meta_modifiedIds, 'w')
    f.write('{}')
    f.close()
    click.echo("modifiedID cleaned")
