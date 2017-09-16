from __future__ import print_function

import grpc

import msg_pb2
import msg_pb2_grpc
import random


def run():

    with open('server.crt') as f:
        trusted_certs = f.read()
    creds = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
    channel = grpc.secure_channel('localhost:50051', creds)
    stub = msg_pb2_grpc.OrpheusStub(channel)
    metadata = [(b'db', b'sixpluszero'), (b'user', b'sixpluszero'), (b'password', b'')]
    '''
    response = stub.Init(request=msg_pb2.InitRequest(datafile = "test/data.csv", cvd = "dataset6", schema = "test/sample_schema.csv"), metadata=metadata)
    print(response.msg)
    response = stub.List(request=msg_pb2.Empty(), metadata=metadata)
    print(response.msg)
    '''
    '''
    response = stub.Drop(request=msg_pb2.DropRequest(cvd="dataset6"), metadata=metadata)
    print(response.msg)
    '''
    response = stub.List(request=msg_pb2.Empty(), metadata=metadata)
    print(response.msg)

if __name__ == '__main__':
  run()
