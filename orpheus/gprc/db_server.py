from concurrent import futures
import time
import grpc
import msg_pb2
import msg_pb2_grpc
from db_manager import DatabaseManager
from orpheus.core.orpheus_sqlparse import SQLParser
from orpheus.core.executor import Executor

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

with open('server.key') as f:
    private_key = f.read()
with open('server.crt') as f:
    certificate_chain = f.read()

class Orpheus(msg_pb2_grpc.OrpheusServicer):    
    def config(self, context):
        conf_arr = context.invocation_metadata()
        conf = {}
        for i in conf_arr:
            conf[i[0]] = i[1]
        conf['host'] = 'localhost'
        conf['port'] = 5432
        conf['orpheus_home'] = '/Users/sixpluszero/work/implementation/'
        conf['log_path'] = conf['orpheus_home'] + '.meta/logs/log.out'
        conf['user_log'] = conf['orpheus_home'] + '.meta/user_log'
        conf['commit_path'] = conf['orpheus_home'] + '.meta/commit_tables'
        conf['meta_info'] = conf['orpheus_home'] + '.meta/tracker'
        conf['meta_modifiedIds'] = conf['orpheus_home'] + '.meta/modified'
        conf['vGraph_json'] = conf['orpheus_home'] + '.meta/vGraph_json'
        return conf

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
            err_info = "%s created successfully" % (request.datafile)
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
        info = "%s dropped successfully" % (request.cvd)
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
        print executable_sql
        ret = msg_pb2.RunReply()
        try:
            result = conn.execute_sql(executable_sql)
            if parser.is_select(executable_sql):
                for row in result:
                    row_msg = msg_pb2.Record()
                    for col in row:
                        row_msg.columns.append(col)
                    ret.data.rows.extend([row_msg])
                ret.msg = "Query processed successfully."
            else:
                ret.msg = result
        except Exception as e:
            #import traceback
            ret.msg = "Error in execution, please revise.\n"
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
        delimiters = request.delimiters
        header = request.header
        ignore = request.ignore
        if to_table == '':
            to_table = None
        if to_file == '':
            to_file = None    
        if delimiters == '':
            delimiters = ','

        executor.exec_checkout(cvd, vlist, to_table, to_file, delimiters, header, ignore, conn)

        return msg_pb2.BasicReply(msg='Checkout successfully.')

    def commit(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        executor = Executor(conf)
        msg = request.message
        table_name = request.table
        file_name = request.file
        delimiters = request.delimiters
        header = request.header
        if table_name == '':
            table_name = None
        if file_name == '':
            file_name = None
        if delimiters == '':
            delimiters = ','

        executor.exec_commit(msg, table_name, file_name, delimiters, header, conn)

        return msg_pb2.BasicReply(msg='Commit successfully.')
        
def serve():
    server_credentials = grpc.ssl_server_credentials(((private_key, certificate_chain,),))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    msg_pb2_grpc.add_OrpheusServicer_to_server(Orpheus(), server)
    server.add_secure_port('[::]:50051', server_credentials)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
