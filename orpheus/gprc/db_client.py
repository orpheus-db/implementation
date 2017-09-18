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
    
    response = stub.list(request=msg_pb2.Empty(), metadata=metadata)
    print(response.msg)

    response = stub.drop(request=msg_pb2.DropRequest(cvd="dataset7"), metadata=metadata)
    print(response.msg)

    response = stub.list(request=msg_pb2.Empty(), metadata=metadata)
    print(response.msg)

    response = stub.init(request=msg_pb2.InitRequest(datafile = "test/data.csv", cvd = "dataset7", schema = "test/sample_schema.csv"), metadata=metadata)
    print(response.msg)


    versions = msg_pb2.Versions()
    versions.vals.append(1)
    response = stub.checkout(request = msg_pb2.CheckoutRequest(cvd = 'dataset7', version = versions, file = 'checkout6.csv'), metadata=metadata)
    print(response.msg)

    
    response = stub.commit(request = msg_pb2.CommitRequest(file = 'checkout6.csv', message = 'this is my checkout'), metadata=metadata)
    
    query = "SELECT employee_id, age FROM VERSION 1,2 OF CVD dataset1"
    response = stub.run(request = msg_pb2.RunRequest(query = query), metadata=metadata)

    for row in response.data.rows:
        row_str = [(col) for col in row.columns]
        print(row_str)

    response = stub.drop(request=msg_pb2.DropRequest(cvd="dataset7"), metadata=metadata)
    print(response.msg)

if __name__ == '__main__':
  run()
