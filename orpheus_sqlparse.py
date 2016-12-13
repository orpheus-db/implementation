# this class uses the sqlparse library to extract the semantics of OrpheusDB SQL statement

import sqlparse, re
from sqlparse.sql import Identifier, Token, Where

import orpheus_const as const
from relation import RelationManager


class InvalidSyntaxError(Exception):
  def __init__(self, statement):
      self.statement = statement
  def __str__(self):
      return "Error parsing '%s'" % self.statement


# Hard coded table alias
INDEXTABLE_ALIAS = 'i'
DATATABLE_ALIAS = 'd'
VERSIONTABLE_ALIAS = 'v'

# predefined list of tokens
WHITESPACE_tok = Token(sqlparse.tokens.Whitespace, ' ')
WHERE_tok = Token('', 'WHERE')


RID_INDEX_tok = Token('', "d.rid = ANY(i.rlist)")


class SQLParser(object):

	def __init__(self, conn):
		self.conn = conn
		self.relation = RelationManager(self.conn)
		self.reserved_column_names = ['cvd']

	def construct_identifier(self, content):
		return Identifier([Token('', content)])

	def get_fields_mapping(self, attributes):
		# mapping from attribute name to corresponding table
		# by default, d = datatable, i = indextable, v = versiontable
		fields_mapping = {'vid' : 'i'}
		for attribute in attributes:
			fields_mapping[attribute] = 'd'

		versiontable_attributes = ["author", "num_records", "parent", "children", "create_time", "commit_time", "commit_msg"] 
		# take in version table attributes
		for version_attribute in versiontable_attributes:
			fields_mapping[version_attribute] = 'v'
		
		return fields_mapping

	def get_touched_table(self, touched_columns, fields_mapping):
		touched_table = set()
		for column in touched_columns.keys():
			try:
				touched_table.add(fields_mapping[column])
			except KeyError:
				pass # user defined alias
		return touched_table

	# anything in this parsed statement
	def get_touched_column_names(self, parent, stop_words=set()):
		tokens = parent.flatten()
		column_names = {}
		for token in tokens:
			if token.ttype == sqlparse.tokens.Name:
				# this is a column
				column_value = token.value
				if column_value not in stop_words:
					token_parent = token.parent
					token_index = token_parent.token_index(token)
					column_names[column_value] = (token_parent, token_index)
		return column_names

	# return replaced from clause
	def get_from_clause(self, dataset_name, touched_table):
		# rule based !
		datatable = dataset_name + const.DATATABLE_SUFFIX
		indextable = dataset_name + const.INDEXTABLE_SUFFIX
		versiontable = dataset_name + const.VERSIONTABLE_SUFFIX
		if 'd' in touched_table and 'i' in touched_table:
			return "%s, %s" % (datatable + ' d', indextable + ' i')
		elif 'v' in touched_table and 'i' in touched_table:
			return "%s, %s" % (versiontable + ' v', indextable + ' i')
		elif 'd' in touched_table and len(touched_table) == 1: # meaning there is only datatable attributes are touched
			return "%s" % datatable + ' d'
		elif 'v' in touched_table and len(touched_table) == 1: # meaning there is only versiontable attributes are touched
			return "%s" % versiontable + ' v'
		else:
			return "%s, %s, %s" % (versiontable + ' v', indextable + ' i', datatable + ' d')


	def get_where_clause(self, touched_table):
		# rule based!
		if 'd' in touched_table and 'i' in touched_table:
			return "d.rid = ANY(i.rlist)"
		else:
			return None


	# return the first occurence of verions (1,2), OF cvd (ds1)
	def get_dataset_name_and_versions(self, parent):
		tokens = parent.tokens
		# unnest parenthesis
		lis = []
		for token in tokens:
			if type(token) == sqlparse.sql.Parenthesis:
				# print "line 68"
				lis.extend(token.tokens)
			else:
				lis.append(token)
		# find the keyword VERSION and set its parent
		vlist, dataset_name, parent, version_idx = [], "", None, None
		for i,token in enumerate(lis):
			if token.value == 'version':
				parent = token.parent
				version_idx = parent.token_index(token)
				vlist = lis[i+2].value
				dataset_name = lis[i+6].value.split()[-1]
				# print "line 80"
				break
				# vlist_id = lis[i+2].ttype
				# if vlist_id is tokens.Number.Integer:
				# 	vlist = lis[i+2].value
				# 	dataset_name = lis[i+6].value.split()[-1]
				# elif:
		return vlist, dataset_name, parent, version_idx

	# find the first occurence of CVD, return the name of CVD, its handle and index
	def find_cvd_handle(self, parent):
		tokens = list(parent.flatten())
		parent, dataset_name, cvd_index = None, None, None
		for i,token in enumerate(tokens):
			if token.value == 'cvd':
				# found the clause, need to find its parent handle
				parent = token.parent
				dataset_name = tokens[i + 2].value
				while type(parent) != sqlparse.sql.Parenthesis and type(parent) != sqlparse.sql.Statement:
					# stops when we find a handle to either () or statement
					token = parent
					parent = parent.parent # traverse up tree
				cvd_index = parent.token_index(token)
				break
		return dataset_name, parent, cvd_index

	# find the Where clause index under parent.tokens
	def find_where_index(self, parent):
		lis = parent.tokens
		for i, token in enumerate(lis):
			if type(token) is sqlparse.sql.Where:
				return i
		return -1

	# find the place in parent to insert WHERE clause
	def find_where_insert(self, parent):
		lis = parent.tokens
		for i,token in enumerate(lis):
			if token.value == 'group' or token.value == 'order':
				return i - 1 # anything that before group by or order by, -1 for the space
		return len(lis) if lis[-1].value != ')' else len(lis) - 1


	# version known, replace the tokens
	def replace_known_version(self, dataset_name, vlist, parent, version_idx):
		# if parent has where, append a new where
		datatable = dataset_name + const.DATATABLE_SUFFIX
		indextable = dataset_name + const.INDEXTABLE_SUFFIX
		rlist = self.relation.select_records_of_version_list(vlist.split(','), indextable)
		constraint = "rid = ANY('%s'::int[])" % rlist

		# replace the FROM clause
		parent.tokens = parent.tokens[:version_idx] + [self.construct_identifier(datatable)] + parent.tokens[version_idx + 7:]

		# replace the WHERE clause
		where_indx = self.find_where_index(parent)
		if where_indx < 0:
			new_idex = self.find_where_insert(parent) # find the place to insert new where
			parent.insert_before(new_idex, self.construct_identifier(" where " + constraint))
		else:
			where_token = parent.tokens[where_indx]
			where_token.tokens.extend(self.construct_identifier(" and " + constraint))


	def replace_unknown_version(self, parent, cvd_idx, dataset_name, fields_mapping, touched_column_names):
		# find touched tables
		touched_table = self.get_touched_table(touched_column_names, fields_mapping)
		table_constraint = self.get_from_clause(dataset_name, touched_table)

		# replace the from clause
		parent.tokens = parent.tokens[:cvd_idx] + [self.construct_identifier(table_constraint)] + parent[cvd_idx+1:]
		where_constraint = self.get_where_clause(touched_table)

		# replace the where clause
		where_indx = self.find_where_index(parent)
		if where_indx < 0:
			# no where, needs to add
			new_idex = self.find_where_insert(parent) # find the place to insert new where
			parent.insert_before(new_idex, self.construct_identifier(" where " + where_constraint))
		else:
			where_token = parent.tokens[where_indx]
			where_token.tokens.extend(self.construct_identifier(" and " + where_constraint))

		# replace all the touched columns by prefix a alias
		for column in touched_column_names.keys():
			(column_parent, column_idx) = touched_column_names[column]
			if column in fields_mapping: # only those we found in tables
				# replace them
				mapped_table_alias = fields_mapping[column]
				column_parent.tokens = column_parent.tokens[:column_idx] + [self.construct_identifier("%s.%s" % (mapped_table_alias, column))] + column_parent.tokens[column_idx + 1:]



	# main method to parse
	# all char casted to lower case
	def parse(self, raw_sql):
		relation = RelationManager(self.conn)
		line = raw_sql.lower()
		try:
			# two cases
			# 1. version is specified, version 1,2 from cvd ds1
			# 2. version is not specified, from CVD 
			# TODO: add more cases? 
			version_specified_re = re.compile('.*?from\sversion\s(\d+|\d+(,\d+)+)\sof\scvd\s(\w+);?')
			version_matched = version_specified_re.match(line)
			if version_matched: # found case 1
				# vlist = version_matched.group(1) # list of version separted by comma
				# dataset_name = version_matched.group(3) # whatever after keyword CVD
				parsed_statement = sqlparse.parse(line)[0]
				vlist, dataset_name, parent, version_idx = self.get_dataset_name_and_versions(parsed_statement)
				self.replace_known_version(dataset_name, vlist, parent, version_idx)
				return str(parsed_statement)
			version_unknown_re = re.compile('.*from\scvd\s(\w+);?')
			version_unknown_matched = version_unknown_re.match(line)
			if version_unknown_matched: # found case 2
				parsed_statement = sqlparse.parse(line)[0]
				dataset_name, parent, cvd_idx = self.find_cvd_handle(parsed_statement)

				datatable_attributes, _ = self.relation.get_datatable_attribute(dataset_name + const.DATATABLE_SUFFIX)

				# get the mapping from each field to alias
				fields_mapping = self.get_fields_mapping(datatable_attributes)

				print fields_mapping

				touched_column_names = self.get_touched_column_names(parent, stop_words=set(self.reserved_column_names + [dataset_name]))

				print touched_column_names

				self.replace_unknown_version(parent, cvd_idx, dataset_name, fields_mapping, touched_column_names)

				return str(parsed_statement)
			# print parsed_statement

		except:
			import traceback
			traceback.print_exc()
			raise InvalidSyntaxError(raw_sql)
			return