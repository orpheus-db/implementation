from __future__ import print_function

import grpc

import msg_pb2
import msg_pb2_grpc
import random

hostname = "hilda.cs.illinois.edu"
#hostname = "localhost"
port = 1503

# This method aims to test the gRPC server APIs
def run():

    channel = grpc.insecure_channel('%s:%d' % (hostname, port))
    stub = msg_pb2_grpc.OrpheusStub(channel)

    # Postgres login information 
    metadata = [(b'db', b'postgres'), (b'user', b'postgres'), (b'password', b'postgres')]
    
    print ("========")
    response = stub.list(request=msg_pb2.ListRequest(), metadata=metadata)
    print("List the CVDs in OrpheusDB: %s" % response.msg)

    print ("========")
    response = stub.init(request=msg_pb2.InitRequest(datafile = "test/protein.csv", cvd = "protein", schema = "test/protein_schema.csv"), metadata=metadata)
    print("Initialize a dataset in OrpheusDB as CVD protein... \n%s" % response.msg)

    print ("========")
    versions = msg_pb2.Versions()
    versions.vals.append(1)
    response = stub.checkout(request = msg_pb2.CheckoutRequest(cvd = 'protein', version = versions, file = 'protein_tmp.csv'), metadata=metadata)
    print(response.msg)

    print ("========")
    response = stub.commit(request = msg_pb2.CommitRequest(file = 'protein_tmp.csv', message = 'this is my checkout'), metadata=metadata)
    print(response.msg)

    print ("========")
    query = "SELECT protein1, protein2, neighborhood FROM VERSION 1,2 OF CVD protein WHERE neighborhood > 300"

    print ("Executing the following query: %s" % query)
    response = stub.run(request = msg_pb2.RunRequest(query = query), metadata=metadata)

    for row in response.data.rows:
        row_str = [(col) for col in row.columns]
        print(row_str)

    print ("========")
    response = stub.drop(request=msg_pb2.DropRequest(cvd="protein"), metadata=metadata)
    print("Dropping the protein.. \n%s" % response.msg)

    response = stub.create_user(request=msg_pb2.CreateUserRequest(user="abc", password="def"), metadata=metadata)
    print(response.msg)
if __name__ == '__main__':
  run()
