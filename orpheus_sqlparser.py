# This class contains a class called SQLParser that handles the parsing of richer syntax of SQL.
import re
import orpheus_const as const
from relation import RelationManager

class InvalidSyntaxError(Exception):
  def __init__(self, statement):
      self.statement = statement
  def __str__(self):
      return "Error parsing '%s'"

class SQLParser(object):

	# takes in a connection object
	def __init__(self, conn):
		self.conn = conn
		self.lis_key_words = "UNION,ALL,AND,INTERSECT,EXCEPT,COLLATE,ASC,DESC,ON,USING,NATURAL,INNER,CROSS,LEFT,OUTER,JOIN,AS,INDEXED,NOT,SELECT,DISTINCT,FROM,WHERE,GROUP,BY,HAVING,ORDER,BY,LIMIT,OFFSET,COUNT,MAX,MIN"
		self.lis_punct = ",.;()|><=+-"

	# rule-based to extract column name touched by this sql statement
	def column_names(self, raw_statement):
		# replace all reserved keywords and find the remaining fields word

		# replace sql keywords
		replacement_dict = {}
		for keyword in self.lis_key_words.split(','):
			replacement_dict[keyword] = ''
		return_str = re.sub(r'\b(\w+)\b', lambda m:replacement_dict.get(m.group(1), m.group(1)), raw_statement)

		# replace the unnecessary punctation
		for punct in self.lis_punct:
			 return_str = return_str.replace(punct, ' ')

		# replace the unnecessary fixed int and text
		# TODO: add more stuff here, timestamp?
		return_str = re.sub(r'\b(\d+)\b', '', return_str) # int value
		return_str = re.sub(r'\b(\"\w+\")\b', '', return_str) # predefined text

		return set(return_str.split())

	def get_fields_mapping(self, dataset_name, attributes):
		# mapping from attribute name to corresponding table
		# by default, d = datatable, i = indextable, v = versiontable
		fields_mapping = {'vid' : 'i'}
		for attribute in attributes:
			fields_mapping[attribute] = 'd'
		return fields_mapping
		


	# main logic to transfer line to executable sql statments, rule based
	def parse(self, line):
		# two cases
		# 1. version is specified, version 1,2 from cvd ds1
		# 2. version is not specified
		version_specified_re = re.compile('.*FROM\sVERSION\s(\d+|\d+(,\d+)+)\sOF\sCVD\s(\w+);?')
		version_matched = version_specified_re.match(line)
		if version_matched:
			replacement_re = re.compile('FROM\sVERSION\s(\d+|\d+(,\d+)+)\sOF\sCVD\s(\w+)')
			# this line contains FROM VERSION * OF CVD *
			vlist = version_matched.group(1) # list of version separted by comma
			dataset_name = version_matched.group(3) # whatever after keyword CVD
			datatable = dataset_name + const.DATATABLE_SUFFIX
			indextable = dataset_name + const.INDEXTABLE_SUFFIX
			relation = RelationManager(self.conn)
			rlist = relation.select_records_of_version_list(vlist.split(','), indextable)
			replacement_from_clause = "FROM %s WHERE rid = ANY('%s'::int[])" % (datatable, rlist)
			return re.sub(replacement_re, replacement_from_clause, line)
		version_unknown_re = re.compile('.*FROM\sCVD\s(\w+);?')
		version_unknown_matched = version_unknown_re.match(line)
		if version_unknown_matched:
			replacement_re = re.compile('FROM\sCVD\s(\w+);?')

			dataset_name = version_unknown_matched.group(1) # whatever after keyword CVD
			datatable = dataset_name + const.DATATABLE_SUFFIX
			indextable = dataset_name + const.INDEXTABLE_SUFFIX
			versiontable = dataset_name + const.VERSIONTABLE_SUFFIX

			relation = RelationManager(self.conn)
			datatable_attributes, _ = relation.get_datatable_attribute(dataset_name + const.DATATABLE_SUFFIX)

			# get the mapping from each field to alias
			fields_mapping = self.get_fields_mapping(dataset_name, datatable_attributes)

			# get list of touched columns
			list_of_column = self.column_names(re.sub(replacement_re, "", line))

			line = re.sub(replacement_re, 'FROM %s d, %s i, %s v' % (datatable, indextable, versiontable), line)

			print list_of_column
			print fields_mapping
			for column in list_of_column:
				print column
				if column == '*':
					# what should i do?
					pass
				elif column == 'vid':
					line = line.replace('vid', 'unnest(i.vlist)')
					print 'hello'
				else:
					try:
						line = line.replace(column, fields_mapping[column] + '.' + column)
					except KeyError: # means user defined alias
						pass # do not replace it 
			print line
			