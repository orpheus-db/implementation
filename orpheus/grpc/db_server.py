from concurrent import futures
import time
import grpc
import msg_pb2
import msg_pb2_grpc
import os
import yaml
from db_manager import DatabaseManager
from orpheus.core.orpheus_sqlparse import SQLParser
from orpheus.core.executor import Executor
from orpheus.core.orpheus_exceptions import BadStateError, NotImplementedError, BadParametersError

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

hostname = "0.0.0.0"
port = 8888
with open('server.key') as f:
    private_key = f.read()
with open('server.crt') as f:
    certificate_chain = f.read()

class Orpheus(msg_pb2_grpc.OrpheusServicer):
    def config(self, context):
        self.config_file = 'config.yaml'
        if 'ORPHEUS_HOME' not in os.environ:
            os.environ['ORPHEUS_HOME'] = os.getcwd()
        self.config_path = os.environ['ORPHEUS_HOME'] + '/' + self.config_file
        try:
            with open(self.config_path, 'r') as f:
                self.config_yaml = yaml.load(f)

            assert(self.config_yaml['orpheus_home'] != None)

            if not self.config_yaml['orpheus_home'].endswith("/"):
                self.config_yaml['orpheus_home'] += "/"
            # if user overwrite the ORPHEUS_HOME, rewrite the enviormental parameters
            if 'orpheus_home' in self.config_yaml:
                os.environ['ORPHEUS_HOME'] = self.config_yaml['orpheus_home']
        except (IOError, KeyError) as e:
            raise BadStateError("config.yaml file not found or data not clean, abort")
            return
        except AssertionError as e:
            raise BadStateError("orpheus_home not specified in config.yaml")
            return
        except: # unknown error
            raise BadStateError("Unknown error during loading the config file, abort")
            return

        for info in context.invocation_metadata():
            self.config_yaml[info[0]] = info[1]
        return self.config_yaml

    def connect_db(self, conf):
        conn = DatabaseManager(conf)
        return conn

    def init(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        executor = Executor(conf)
        err_info = ""
        try:
            executor.exec_init(request.datafile, request.cvd, None, request.schema, conn)
        except Exception as e:
            err_info = str(e)
        if err_info == "":
            err_info = "%s is created successfully" % (request.cvd)
        ret = msg_pb2.BasicReply(msg=err_info)
        return ret

    def list(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        res = conn.list_dataset()
        ret = msg_pb2.BasicReply(msg="%s" % (res))
        return ret

    def drop(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        executor = Executor(conf)
        info = "%s is dropped successfully" % (request.cvd)
        try:
            executor.exec_drop(request.cvd, conn)
        except Exception as e:
            info = str(e)
        ret = msg_pb2.BasicReply(msg="%s" % (info))
        return ret

    def run(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        parser = SQLParser(conn)
        #executor = Executor(conf)
        executable_sql = parser.parse(request.query)
        #print executable_sql
        ret = msg_pb2.RunReply()
        try:
            result = conn.execute_sql(executable_sql)
            if parser.is_select(executable_sql):
                for row in result:
                    row_msg = msg_pb2.Record()
                    for col in row:
                        row_msg.columns.append(col)
                    ret.data.rows.extend([row_msg])
                ret.msg = "The query is processed successfully."
            else:
                ret.msg = result
        except Exception as e:
            #import traceback
            ret.msg = "There are errors in the query execution, please revise.\n"
            #traceback.print_exc()
        return ret

    def checkout(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        executor = Executor(conf)
        cvd = request.cvd

        vlist = ()
        for v in request.version.vals:
            vlist = vlist + (str(v),)

        to_table = request.table
        to_file = request.file
        delimiters = request.delimiters or ","
        header = request.header
        ignore = request.ignore

        executor.exec_checkout(cvd, vlist, to_table, to_file, delimiters, header, ignore, conn)

        return msg_pb2.BasicReply(msg='%s is checked out successfully.' % (to_table or to_file))

    def commit(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        executor = Executor(conf)
        msg = request.message
        table_name = request.table
        file_name = request.file
        delimiters = request.delimiters or ","
        header = request.header


        executor.exec_commit(msg, table_name, file_name, delimiters, header, conn)

        return msg_pb2.BasicReply(msg='%s is committed successfully.' % (table_name or file_name))

def serve():
    server_credentials = grpc.ssl_server_credentials(((private_key, certificate_chain,),))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    msg_pb2_grpc.add_OrpheusServicer_to_server(Orpheus(), server)
    #server.add_secure_port('%s:%d' % (hostname, port), server_credentials)
    server.add_insecure_port('%s:%d' % (hostname, port))
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
