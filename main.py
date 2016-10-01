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


userinfo_file = 'user.info'
# def init_db():
#     try:
#         with open(userinfo_file, 'r') as f:
#             config_info = f.readline()
#         config_info = json.loads(config_info)
#         db_name = config_info['database']
#         user = config_info['user']
#         password = config_info['password']
#         if not db_name:
#             click.echo("Please config user and databse first, use the command config")
#             return
#     except:
#         click.echo("Please config user and database first, use the command config!!!")
#         return
#
#     conn = DatabaseManager()
#     conn.currentDB = db_name
#     conn.user = user
#     conn.password = password
#     conn.connect_db()
#     return conn

@click.group()
@click.pass_context
def cli(ctx):
   pass

@cli.command()
@click.option('--user', '-u', required=True, help='Specify the user name that you want to create.')
@click.option('--password', '-p', required=True, help='Specify the password that you want to use.')
@click.option('--database', '-d', required=True, help='Specify the database that you want to connected with.')
def config(user, password, database):
    # TODO: Need to configure the system
    # (i.e. remember user information after they init on one computer)
    # One solution is to store info into a file (WHERE? SECURITY?)
    # Maybe depends on the APIs of the python package we use. Not sure.
    config_info = {
        'user':user,
        # TODO encrytion needed
        'password':password,
        'database':database
    }
    f = open(userinfo_file, 'w')
    f.write(json.dumps(config_info))
    f.close()
    pass;

@cli.command()
@click.option('--user', '-u', required=True, help='Specify the user name that you want to create.')
@click.option('--password', '-p', required=True, help='Specify the password that you want to use.')
@click.option('--db_name', '-n', help='Specify the database that you want to connect to.')
def create_user(user, password, db_name):
    # TODO: password encryption
    # create user
    UserManager.create_user(user, password)
    # create user in the DB
    DatabaseManager.create_user(user, password, db_name);

@cli.command()
@click.option('--dataset_name', '-n', required=True, help='Specify the dataset what you want to init into the DB')
@click.option('--primary', '-key', required=False,multiple=True,help='Specify the primary key of the dataset')

def init_dataset(dataset_name):
    # By default, we connect to the database specified in the -config- command earlier

    conn = DatabaseManager()

    version = VersionManager(conn)
    version.create_version_graph(dataset_name)
    version.create_index_table(dataset_name)

    relation = RelationManager(conn)
    relation.create_relation(dataset_name)
    # TODO: What about schema? Automation or specified by user?

@cli.command()
@click.option('--vlist', '-v', multiple=True, required=True, help='Specify version you want to clone, use multiple -v for multiple version checkout')
@click.option('--from_table', '-f', required=True, help='Specify the table name to checkout from')
@click.option('--to_table', '-t', required=True, help='Specify the table name to checkout to.')
@click.option('--ignore', '-i', help='If set, clone versions into table will ignore duplicated key', is_flag=True)
def clone(vlist, from_table, to_table, ignore):

    # sanity check
    if not from_table:
        click.echo("Please mention the original table you want to checkout from.")
        return
    if not to_table:
        click.echo("Please mention the original table you want to checkout to.")
        return

    conn = DatabaseManager(userinfo_file)
    relation = RelationManager(conn)
    try:
        relation.checkout_table(vlist, from_table, to_table,ignore)
    except:
        print "DB error"
        click.echo("check the name of tables. relation %s may already exists" % to_table)
        return

    AccessManager.grant_access(to_table, conn.user)
    metadata = MetadataManager(conn)
    metadata.update(to_table,from_table,vlist)

    click.echo("Table %s has been cloned from version %s" % (to_table, ",".join(vlist)))

@cli.command()
@click.option('--msg','-m', help='Commit message', required = True)
@click.option('--table_name','-t', help='Commit table', required = True)
def commit(msg, table_name):
    # sanity check
    if msg is None or len(msg) == 0:
        click.echo("Needs commit msg, abort")
        return
    db_name = ".."

    try:
        conn = DatabaseManager.connect_db(db_name);
    except:
        print "connection to DB failed."
        return

    relation = RelationManager(conn);
    if not relation.check_table_exists(table_name):
        click.echo("Table %s not found, abort" % table_name)
        return

    # We need to get the derivation information of the committed table;
    # Otherwise, in the multitable scenario, we do not know which datatable/version_graph/index_table
    # that we need to update information.
    parent_name = "..."

    # update corresponding version graph
    version = VersionManager(conn);
    try:
        graph_name = parent_name + "_version_graph"
        version.update_version_graph(graph_name);
    except:
        print "update version graph error"
        return

    # update index table
    try:
        index_name = parent_name + "_index_table"
        version.update_index_table(index_name);
    except:
        print "update index table error"
        return

    # append new records to the datatable
    try:
        data_table_name = parent_name + "_datatable_name"
        relation.update_datatable(data_table_name);
    except:
        print "update datatable error"
        return

    # TODO: Before return, we may also need to clean table if any.

    click.echo("commited")



@cli.command()
def clean():
    conn = DatabaseManager(userinfo_file)
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
