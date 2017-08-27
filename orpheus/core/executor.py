import sys
import user
import json
import pandas as pd
import os
import sqlparse

from db import DatasetExistsError
from relation import RelationManager, RelationNotExistError, RelationOverwriteError, ReservedRelationError
from orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError


from access import AccessManager
from version import VersionManager
from metadata import MetadataManager
from user_control import UserManager
from orpheus_schema_parser import Parser as SimpleSchemaParser
from helper import Print
import orpheus_const as const



class Executor(object):
	def __init__(self, config, request = False):
		self.config = config
		self.request = request
		self.p = Print(request)

	def exec_init(self, input_file, dataset, table, schema, conn):
	    try:
	        rel = RelationManager(conn)

	        if (not table and not schema) or (table and schema):
	            raise BadParametersError("Need either (not both) a table or a schema file")
	            return

	        abs_path = self.config['orpheus_home'] + schema if schema and schema[0] != '/' else schema

	        if table:
	            attribute_name , attribute_type = rel.get_datatable_attribute(table)
	        else:
	            attribute_name , attribute_type = SimpleSchemaParser.get_attribute_from_file(abs_path)

	    except Exception as e:
	        import traceback
	        traceback.print_exc()
	        self.p.perror(str(e))
	        raise Exception
	        return
	    # at this point, we have a valid conn obj and rel obj
	    try:
	        # schema of the dataset, of the type (name, type)
	        schema_tuple = zip(attribute_name, attribute_type)
	        # create new dataset
	        conn.create_dataset(input_file, dataset, schema_tuple, attributes=attribute_name)
	        # get all rids in list
	        lis_rid = rel.select_all_rid(const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX)
	        # init version info
	        version = VersionManager(conn, self.request)

	        version.init_version_graph_dataset(dataset, lis_rid, self.config['user'])
	        version.init_index_table_dataset(dataset, lis_rid)
	    except DatasetExistsError as e:
	        self.p.perror(str(e))
	        return
	    except Exception as e:
	        # revert back to the state before create
	        conn.drop_dataset(dataset)
	        self.p.perror(str(e))
	        return 
	    self.p.pmessage("Dataset %s has been created successful" % dataset)

	def exec_drop(self, dataset, conn):
		# TODO: add a popup window to confirm
		# E.g. if click.confirm('Are you sure you want to drop %s?' % dataset):
		try:
			
			conn.drop_dataset(dataset)
			self.p.pmessage("Dataset %s has been dropped" % dataset)
		except Exception as e:
			self.p.perror(str(e))
			raise Exception
	        return

	def exec_checkout(self, dataset, vlist, to_table, to_file, delimiters, header, ignore, conn):
	    if not to_table and not to_file:
	    	self.p.perror(str(BadParametersError("Need a destination, either a table (-t) or a file (-f)")))
	        return
	    try:
	        relation = RelationManager(conn)
	    except Exception as e:
	    	self.p.perror(str(e))
	        raise Exception
	        return
	    abs_path = self.config['orpheus_home'] + to_file if to_file and to_file[0] != '/' else to_file
	    try:
	        metadata = MetadataManager(self.config, self.request)
	        meta_obj = metadata.load_meta()
 	        datatable = dataset + const.DATATABLE_SUFFIX
	        indextable = dataset + const.INDEXTABLE_SUFFIX
	        relation.checkout(vlist, datatable, indextable, to_table=to_table, to_file=abs_path, delimiters=delimiters, header=header, ignore=ignore)
	        # update meta info
	        AccessManager.grant_access(to_table, conn.user)
	        metadata.update(to_table, abs_path, dataset, vlist, meta_obj)
	        metadata.commit_meta(meta_obj)
	        if to_table:
	        	self.p.pmessage("Table %s has been cloned from version %s" % (to_table, ",".join(vlist)))
	        if to_file:
	         	self.p.pmessage("File %s has been cloned from version %s" % (to_file, ",".join(vlist)))
	    except Exception as e:
	        if to_table and not (RelationOverwriteError or ReservedRelationError):
	        	relation.drop_table(to_table)
	        if to_file:
	            pass # delete the file
	        self.p.perror(str(e))
	        raise Exception
	        return

	def exec_commit(self, message, table_name, file_name, delimiters, header, conn):
		# sanity check
	    if not table_name and not file_name:
	    	self.p.perror(str(BadParametersError("Need a source, either a table (-t) or a file (-f)")))
	        return

	    if table_name and file_name:
	    	self.p.perror(str(NotImplementedError("Can either commit a file or a table at a time")))
	        return

	    try:
	        relation = RelationManager(conn)
	        metadata = MetadataManager(self.config, self.request)
	        version = VersionManager(conn, self.request)
	    except Exception as e:
	    	self.p.perror(str(e))
	    	raise Exception
	        return
	    if table_name and not relation.check_table_exists(table_name):
	    	self.p.perror(str(RelationNotExistError(table_name)))
	    	raise Exception
	        return
	    # load parent information about the table
	    # We need to get the derivation information of the committed table;
	    # Otherwise, in the multitable scenario, we do not know which datatable/version_graph/index_table
	    # that we need to update information.
	    try:
	        abs_path = self.config['orpheus_home']  + file_name if file_name else self.config['orpheus_home']
	        parent_vid_list = metadata.load_parent_id(table_name) if table_name else metadata.load_parent_id(abs_path, mapping='file_map')
	       	self.p.pmessage("Parent dataset is %s " % parent_vid_list[0])
	       	self.p.pmessage("Parent versions are %s " % ",".join(parent_vid_list[1]))
	    except Exception as e:
	    	self.p.perror(str(e))
	        raise Exception
	        return
	    parent_name = parent_vid_list[0]
	    parent_list = parent_vid_list[1]

	    datatable_name = parent_name + const.DATATABLE_SUFFIX
	    indextable_name = parent_name + const.INDEXTABLE_SUFFIX
	    graph_name = parent_name + const.VERSIONTABLE_SUFFIX
	    try:
	        # convert file into tmp_table first, then set the table_name to tmp_table
	        if file_name:
	            # need to know the schema for this file
	            _attributes, _attributes_type = relation.get_datatable_attribute(datatable_name)

	            relation.create_relation_force('tmp_table', datatable_name, sample_table_attributes=_attributes) # create a tmp table
	            relation.convert_csv_to_table(abs_path, 'tmp_table', _attributes , delimiters=delimiters, header=header) # push everything from csv to tmp_table
	            table_name = 'tmp_table'
	    except Exception as e:
	    	self.p.perror(str(e))
	        raise Exception
	        return


	    if table_name:
	        try:
	            _attributes, _attributes_type = relation.get_datatable_attribute(datatable_name)
	            commit_attributes, commit_type = relation.get_datatable_attribute(table_name)
	            if len(set(_attributes) - set(commit_attributes)) > 0:
	                raise BadStateError("%s and %s have different attributes" % (table_name, parent_name))
	            view_name = "%s_view" % parent_name
	            relation.create_parent_view(datatable_name, indextable_name, parent_list, view_name)
	            existing_rids = [t[0] for t in relation.select_intersection_table(table_name, view_name, commit_attributes)]
	            sql = relation.generate_complement_sql(table_name, view_name, attributes=_attributes)

	            new_rids = relation.update_datatable(datatable_name, sql)
	            relation.drop_view(view_name)

	            self.p.pmessage("Found %s new records" % len(new_rids))
	            self.p.pmessage("Found %s existing records" % len(existing_rids))

	            current_version_rid = existing_rids + new_rids

	            # it can happen that there are duplicate in here
	            table_create_time = metadata.load_table_create_time(table_name) if table_name != 'tmp_table' else None

	            # update version graph
	            curt_vid = version.update_version_graph(graph_name, self.config['user'], len(current_version_rid), parent_list, table_create_time, message)

	            # update index table
	            version.update_index_table(indextable_name, curt_vid, current_version_rid)
	            self.p.pmessage("Committing version %s with %s records" % (curt_vid, len(current_version_rid)))

	            metadata.update_parent_id(table_name, parent_name, curt_vid) if table_name else metadata.update_parent_id(abs_path, parent_name, curt_vid, mapping='file_map')
	        except Exception as e:
	        	view_name = "%s_view" % parent_name
	        	relation.drop_view(view_name)
	        	self.p.perror(str(e))
	        	raise Exception
	        	return

	    if relation.check_table_exists('tmp_table'):
	        relation.drop_table('tmp_table')

	    self.p.pmessage("Version %s has been committed!" % curt_vid)
	    return parent_name, curt_vid, parent_list

