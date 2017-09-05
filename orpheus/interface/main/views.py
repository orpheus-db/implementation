# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.http import HttpResponse
from django.template import loader,  RequestContext
from django.shortcuts import render
from django.contrib import messages
from src.cmd_parser import Parser
# Create your views here.
from django.shortcuts import render

import json
import os.path, os

from config import CONFIG, DatabaseManager

class tModel:
	def __init__(self, name):
		self.name = name

def index(request):

	context = {}


	table_list, cmd_string = None, ""

	# If command button is clicked
	cmd_btn = request.POST.get('command')

	# If command button is clicked
	explain_btn = request.POST.get('explain')

	# if view button is clicked
	view_btn = request.POST.get('view_btn')
	# if info button is clicked
	info = request.POST.get('info_btn')
	# if diff button is clicked
	diff = request.POST.get('diff_btn')

	# if diff button is clicked
	show_btn = request.POST.get('show_btn')

	# if private_file button
	priv_file_btn = request.POST.get('private_file')

	#if vGraph submit button is clicked
	vGraph_btn =  request.POST.get('vGraph')
	if vGraph_btn:
		selected_cvd = request.POST.get('cvd_selection')
		request.session['prev_selection'] = selected_cvd
	else:
		if 'prev_selection' in request.session:
			prev_selection = request.session['prev_selection']
		else:
			prev_selection = ""
		selected_cvd = prev_selection
	context['selected_cvd'] = selected_cvd

	if view_btn or diff or info or show_btn:
		cmd_string = request.POST.get('cmdExec')
		request.session['cmd_string'] = cmd_string
	else: # cmd_btn:
		cmd_string = request.POST.get('cmdText')
		request.session['cmd_string'] = cmd_string
	context['cmd_string'] = request.session['cmd_string']
	if view_btn or diff or info or cmd_btn or show_btn or explain_btn:
		# Parse and execute the command
		try:
			p = Parser(request)
			if view_btn or diff:
				attributes = p.get_attributes(selected_cvd)
				cmd_string = cmd_string.replace('*', attributes)
				context['cmd_string'] = cmd_string
				request.session['cmd_string'] = cmd_string

			if cmd_string != "":
				table_list = p.parse(cmd_string, explain_btn)
		except Exception as e:
			messages.error(request, str(e))

	fpath = '.meta/vGraph_json/%s' % selected_cvd
	if os.path.isfile(fpath):
		data = json.loads(open(fpath).read())
		data = json.dumps(data)
		context['vGraph_json'] = data
		if selected_cvd:
			request.session['prev_selection'] = selected_cvd
	
	if table_list:
		context['table_list'] = table_list
	if priv_file_btn:
		# open the file with sublime
		try:
			p = Parser(request)
			fpath = p.config['orpheus_home'] + priv_file_btn
			if os.path.exists(fpath):
				os.system('subl %s' % fpath) #TODO: Change to open with default editor after demo
			else:
				messages.error(request, "Unable to open file %s with the path %s" % (priv_file_btn, fpath))
		except Exception as e:
			messages.error(request, str(e))

	print "Reach Here"
	conn = DatabaseManager(CONFIG)
	cvd_sql = "SELECT * FROM %s.datasets" % (CONFIG["user"])
	context['cvds'] =  [r[0] for r in conn.sql_records(cvd_sql)]
	context['files'] = []
	context['tables'] = []

	return render(request, 'main/index.html', context)