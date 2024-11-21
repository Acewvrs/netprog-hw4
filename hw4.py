#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc
from csci4220_hw4_pb2_grpc import KadImplServicer

class KadImpl(csci4220_hw4_pb2_grpc.KadImplServicer):
    def __init__(self, node_id, k_bucket):
        """
        Initialize the KadImpl servicer.
        :param node_id: ID of the current node
        :param k_bucket: List of known nodes (K-Bucket)
        """
        self.node_id = node_id
        self.k_bucket = k_bucket  # This should be a list of Node objects

    def FindNode(self, request, context):
        
        print(f'received request from {request.node.id}')

        response = csci4220_hw4_pb2.NodeList(
            responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=12345, address="127.0.0.1"),
            # nodes=closest_nodes,
        )
        return response
        # """
        # Implements the FindNode RPC.
        # :param request: IDKey object containing the target node ID.
        # :param context: gRPC context.
        # :return: NodeList object containing the closest nodes.
        # """
        # target_id = request.idkey  # The ID we are trying to locate

        # # Sort the k-bucket by distance to the target ID
        # sorted_nodes = sorted(self.k_bucket, key=lambda node: abs(node.id - target_id))

        # # Select the closest K nodes (assuming K is a fixed size)
        # K = 3  # Replace with the appropriate K value
        # closest_nodes = sorted_nodes[:K]

        # Build the NodeList response
        # response = pb2.NodeList(
        #     responding_node=pb2.Node(id=self.node_id, port=12345, address="127.0.0.1"),
        #     nodes=closest_nodes,
        # )
        # return response

def run():
    if len(sys.argv) != 4:
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)

    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
    k = int(sys.argv[3])
    my_hostname = socket.gethostname() # Gets my host name
    my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # kad_node = KadImplServicer()
    kad_node = KadImpl(local_id, [])
    csci4220_hw4_pb2_grpc.add_KadImplServicer_to_server(kad_node, server)

    # server.add_insecure_port(f'[::]:{my_port}')
    server.add_insecure_port(f'{my_address}:{my_port}')
    server.start()

    ''' Use the following code to convert a hostname to an IP and start a channel
    Note that every stub needs a channel attached to it
    When you are done with a channel you should call .close() on the channel.
    Submitty may kill your program if you have too many file descriptors open
    at the same time. '''
    
    remote_addr = socket.gethostbyname(my_address)
    remote_port = int(my_port)
    channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))
    
    stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
    request = csci4220_hw4_pb2.IDKey(
        node= csci4220_hw4_pb2.Node(id=1, port=int(my_port), address=my_address),  # Example node
        idkey=12345  # Example ID key to search for
    )

    try:
        response = stub.FindNode(request)
        print("got a response", response)
        # response = stub.JoinNetwork(csci4220_hw4_pb2.NodeIDRequest(node_id=local_id))
        # print(f"Node {local_id}: Received response from remote node {response.node_id}")
        print("connected!")
    finally:
        channel.close()

    # Keep server running (to handle incoming requests)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"Node {local_id}: Shutting down server.")
        server.stop(0)


if __name__ == '__main__':
    run()

