import shlex
import os
import yaml

from django.contrib import messages
from main.models import CVDs, PrivateFiles, PrivateTables
from django.conf import settings

from orpheus.core.executor import Executor
from orpheus.core.orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError
from orpheus.core.relation import RelationManager
from db import DatabaseManager
import orpheus.core.orpheus_const as const
from orpheus.core.vgraph import VersionGraph


class CommandNotExistError(Exception):
    def __init__(self, cmd):
        self.name = cmd
    def __str__(self):
        return "Command '%s' does not exist" % self.name

class CommandInvalidError(Exception):
    def __init__(self, cmd):
        self.name = cmd
    def __str__(self):
        return "Command '%s' is not valid" % self.name

class Parser(object):
    def __init__(self, request):
        self.request = request
        self.config_file = 'config.yaml'
        if 'ORPHEUS_HOME' not in os.environ:
            os.environ['ORPHEUS_HOME'] = os.getcwd()
            self.config_path = os.environ['ORPHEUS_HOME'] + '/' + self.config_file
        else:
            self.config_path = os.environ['ORPHEUS_HOME'] + self.config_file
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

        # extract database related info from Django setting
        self.config['user'] = settings.DATABASES['default']['USER']
        self.config['database'] = settings.DATABASES['default']['NAME']

    def get_attributes(self, dataset):
        conn = DatabaseManager(self.config, self.request)
        rel = RelationManager(conn)
        datatable_attributes, _ = rel.get_datatable_attribute(dataset + const.DATATABLE_SUFFIX)
        return ",".join(datatable_attributes)

    def parse(self, cmd_string, explain_btn):
        # Preserve the string in commit command
        cmd = shlex.split(cmd_string)
        executor = Executor(self.config, self.request)
        if cmd[0] != "orpheus" or len(cmd) < 2:
            raise CommandNotExistError(cmd)
            return
        action = cmd[1]
        if action == "run" and explain_btn:
            action = "explain"
        elif explain_btn:
            messages.error(self.request, "This command could not be executed by the \'Explain\' button")
            return None
        try:
            if action == "init":
                input_file, dataset, table_name, schema = self.__parse_init(cmd)

                conn = DatabaseManager(self.config, self.request)
                executor.exec_init(input_file, dataset, table_name, schema, conn)
                '''
                try:
                    obj = CVDs.objects.get(name = dataset)
                except CVDs.DoesNotExist:
                    obj = CVDs(name = dataset)
                    obj.save()
                '''

            elif action == "checkout":
                dataset, vlist, to_table, to_file, delimiters, header, ignore = self.__parse_checkout(cmd)
                conn = DatabaseManager(self.config, self.request)
                executor.exec_checkout(dataset, vlist, to_table, to_file, delimiters, header, ignore, conn)
                '''
                if to_table:
                    try:
                        obj = PrivateTables.objects.get(name = to_table)
                    except PrivateTables.DoesNotExist:
                        obj = PrivateTables(name = to_table)
                        obj.save()
                if to_file:
                    try:
                        obj = PrivateFiles.objects.get(name = to_file)
                    except PrivateFiles.DoesNotExist:
                        obj = PrivateFiles(name = to_file)
                        obj.save()
                '''
            elif action == "commit":
                message, table_name, file_name, delimiters, header = self.__parse_commit(cmd)
                conn = DatabaseManager(self.config, self.request)
                parent_name, curt_vid, parent_list = executor.exec_commit(message, table_name, file_name, delimiters, header, conn)


            elif action == "run":
                sql = self.__parse_run(cmd)
                conn = DatabaseManager(self.config, self.request)
                attr_names, transactions = executor.exec_run(sql, conn)
                table_list = []
                table_list.append((attr_names, transactions))
                return table_list
            elif action == "explain":
                sql = self.__parse_run(cmd)
                conn = DatabaseManager(self.config, self.request)
                return executor.exec_explain(sql, conn)
            elif action == "drop":
                dataset = self.__parse_drop(cmd)
                conn = DatabaseManager(self.config, self.request)
                executor.exec_drop(dataset, conn)
                '''
                CVDs.objects.filter(name=dataset).delete()
                '''
            elif action == "show":
                dataset = self.__parse_show(cmd)
                conn = DatabaseManager(self.config, self.request)
                return executor.exec_show(dataset, conn)
            elif action == "restore":
                conn = DatabaseManager(self.config, self.request)
                executor.exec_restore(conn)
            else:
                raise CommandNotExistError(cmd_string)
                return
        except Exception as e:
            messages.error(self.request, str(e))
        return None
    # TODO: This simple parser does not detect invalid optional tags.
    # E.g. -z schema => no schema file detected. Rather, it should print out error message, no -z option

    # Init command
    # Required args: input, dataset, optional args: -t table, -s schema
    def __parse_init(self, cmd):
        try:
            input_file, dataset, table_name, schema = cmd[2], cmd[3], None, None
            if '-t' in cmd:
                table_name = cmd[cmd.index('-t') + 1]
            if '-s' in cmd:
                schema = cmd[cmd.index('-s') + 1]
        except Exception as e:
            raise CommandInvalidError(' '.join(cmd))
            return
        return input_file, dataset, table_name, schema

    # checkout command
    # required argument: dataset, vlist, optional args: to_table, to_file, delimiters, header, ignore
    def __parse_checkout(self, cmd):
        try:
            dataset, vlist = cmd[2], []
            to_table, to_file, delimiters, header, ignore = None, None, ',', False, False
            vlist_indices = [i for i, x in enumerate(cmd) if x == '-v']
            for i in vlist_indices:
                vlist.append(str(cmd[i+1]))
            if '-t' in cmd:
                to_table = cmd[cmd.index('-t') + 1]
            if '-f' in cmd:
                to_file = cmd[cmd.index('-f') + 1]
            if '-d' in cmd:
                delimiters = cmd[cmd.index('-d') + 1]
            if '-h' in cmd:
                header = True
            if '--ignore' in cmd:
                ignore = True
        except Exception as e:
            raise CommandInvalidError(' '.join(cmd))
            return
        return dataset, vlist, to_table, to_file, delimiters, header, ignore

    # Commit command
    # Required argument: -m message, optional argument: -t table_name, -f file_name, -d delimiters, -h header
    def __parse_commit(self, cmd):
        try:
            message = cmd[cmd.index('-m') + 1]
            table_name, file_name, delimiters, header = None, None, ',', False
            if '-t' in cmd:
                table_name = cmd[cmd.index('-t') + 1]
            if '-f' in cmd:
                file_name = cmd[cmd.index('-f') + 1]
            if '-d' in cmd:
                delimiters = cmd[cmd.index('-d') + 1]
            if '-h' in cmd:
                header = True
        except Exception as e:
            raise CommandInvalidError(' '.join(cmd))
            return
        return message, table_name, file_name, delimiters, header

    def __init_vGraph():
        graph = VersionGraph(self.config, self.request)
        try:
            graph.init_vGraph_json(dataset, 1) # init vid = 1
        except Exception as e:
            graph.delete_vGraph_json(dataset)
            raise Exception
            return
        # TODO: What about schema? Automation or specified by user?


    # Drop command
    # Required argument: dataset
    def __parse_drop(self, cmd):
        try:
            dataset = cmd[2]
        except Exception as e:
            raise CommandInvalidError(' '.join(cmd))
            return
        return dataset

    def __parse_run(self, cmd):
        try:
            sql = cmd[2]
        except Exception as e:
            raise CommandInvalidError(' '.join(cmd))
            return
        return sql

    # The func is the same as __parse_drop
    def __parse_show(self, cmd):
        try:
            dataset = cmd[2]
        except Exception as e:
            raise CommandInvalidError(' '.join(cmd))
            return
        return dataset
