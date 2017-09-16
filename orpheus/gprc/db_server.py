from concurrent import futures
import time
import grpc
import msg_pb2
import msg_pb2_grpc
from db_manager import DatabaseManager
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
        conf['log_path'] = '.meta/logs/log.out'
        conf['user_log'] = '.meta/user_log'
        conf['commit_path'] = '.meta/commit_tables'
        conf['meta_info'] = '.meta/tracker'
        conf['meta_modifiedIds'] = '.meta/modified'
        conf['vGraph_json'] = '.meta/vGraph_json'
        return conf

    def connect_db(self, conf):
        conn = DatabaseManager(conf)
        return conn

    def Init(self, request, context):
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

    def List(self, request, context):
        conf = self.config(context)
        conn = self.connect_db(conf)
        res = conn.list_dataset()
        ret = msg_pb2.BasicReply(msg="%s" % (res))
        return ret

    def Drop(self, request, context):
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
