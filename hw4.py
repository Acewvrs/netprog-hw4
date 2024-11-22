#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import grpc
import threading
import time
from collections import deque

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc
from csci4220_hw4_pb2_grpc import KadImplServicer

class KadImpl(csci4220_hw4_pb2_grpc.KadImplServicer):
    def __init__(self, node_id, local_address, local_port, k):
        """
        Initialize the KadImpl servicer.
        :param node_id: ID of the current node
        :param k_bucket: List of known nodes (K-Bucket)
        """
        self.node_id = node_id
        self.address = local_address
        self.port = int(local_port)
        self.recently_used_nodes = list() # list of nodes
        self.node_classes = dict() # maps node id to it kade node implementation (KadImpl)
        self.k_buckets = [[] for i in range(4)] # list of nodes, separated by distance
        self.node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        self.k = k

    # def add_to_kbucket(self, retrieved_node):
    #     distance = find_distance_between_nodes(self.node, retrieved_node)
    #     k_idx = get_bucket_index(distance)
    #     self.k_buckets[k_idx].append(retrieved_node)
    
    def remove_from_kbucket(self, node_to_remove):
        distance = find_distance_between_nodes(self.node, node_to_remove)
        k_idx = get_bucket_index(distance)
        self.k_buckets[k_idx].remove(node_to_remove)

    def node_in_bucket(self, node):
        distance = find_distance_between_nodes(self.node, node_to_remove)
        k_idx = get_bucket_index(distance)
        if (node in self.k_buckets[k_idx]): return True
        return False

    # def saveImpl(self, node_id, kad_node):
    #     self.node_classes[node_id] = kad_node

    def update_k_buckets(self, node):
        # calculate bucket index using XOR distance
        distance = find_distance_between_nodes(self.node, node)
        k_idx = get_bucket_index(distance)
        bucket = self.k_buckets[k_idx] # pass by reference; you can directly change k_buckets by modifying bucket

        if node in bucket:
            # if the node is already in the bucket, move it to the tail (most recently used)
            bucket.remove(node)
        elif len(bucket) >= self.k:
            # Bucket is full, remove the least recently used (first) and add new node
            bucket.pop(0)
        bucket.append(node)

    def update_k_buckets_no_removal(self, node):
        # calculate bucket index using XOR distance
        distance = find_distance_between_nodes(self.node, node)
        k_idx = get_bucket_index(distance)
        bucket = self.k_buckets[k_idx] # pass by reference; you can directly change k_buckets by modifying bucket

        if len(bucket) >= self.k:
            # Bucket is full, remove the least recently used (first) and add new node
            bucket.pop(0)
        bucket.append(node)

    # send findNode request to another node we know
    # returns the k closest nodes returned by that node
    def send_FindNode_request(self, address, port):
        channel = grpc.insecure_channel(address + ':' + str(port))
        
        local_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
        request = csci4220_hw4_pb2.IDKey(
            node=local_node,
            idkey=self.node_id  # ID key to search for
        )
        response = stub.FindNode(request)
        return response.nodes

    def FindNode(self, request, context):
        print(f'Serving FindNode({request.idkey}) request for {request.node.id}')

        # print(request)
        self.update_k_buckets(request.node)

        target_id = request.idkey  # The ID we are trying to locate
        known_nodes = [node for nodes in self.k_buckets for node in nodes]
        visited = [request.node]
        node_found = False
        # sorted(known_nodes, key=lambda n: find_distance_between_nodes(target_id, n))[:self.k]
        # closest_nodes = deque(self.get_k_closest(node_id, known_nodes), maxlen=self.k)
        # closest_nodes = [node for nodes in self.k_buckets for node in nodes if node not in visited]

        # get k closest IDs to nodeID
        k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_nodes(self.node, n))[:self.k]
        while (len(visited) < len(known_nodes) and not node_found):
            # get nodes we've not visited
            nodes_to_visit = [node for node in k_closest_nodes if node.id not in visited]
            # Sort the k-bucket by distance, which is determined by XOR
            # closest_node = sorted(self.nodes, key=lambda node: find_distance_between_nodes(node.id, target_id))
            # print("nodes to visit: ", nodes_to_visit)
            for node in nodes_to_visit:
                visited.append(node.id)

                response_nodes = self.send_FindNode_request(node.address, node.port)
                
                self.update_k_buckets(node)
                
                # Update k-buckets with all nodes in the response
                for response_node in response_nodes:
                    # If a node in R was already in a k-bucket, its position does not change.
                    self.update_k_buckets_no_removal(response_node)

                # concatenate list
                known_nodes.extend(response_nodes)                  

            # get k closest IDs to nodeID
            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_nodes(target_id, n))[:self.k]
        
            # if (k_closest_nodes[0].id == target_id):
            #     break
            for node in k_closest_nodes:
                if (node.id == target_id):
                    node_found = True
                    break

        # self.update_k_buckets(request.node)

        # closest_nodes = [node for node in k_closest_nodes if ]
        # print(k_closest_nodes)

        response = csci4220_hw4_pb2.NodeList(
            responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
            nodes=k_closest_nodes
        )
        return response
        ''' 
        While some of the k closest nodes to <nodeID> have not been asked:
            S = the k closest IDs to <nodeID>
            S' = nodes in S that have not been contacted yet
            For node in S':
                R = node.FindNode(<nodeID>)
                # Always mark node as most recently used
                Update k-buckets with node
                # If a node in R was already in a k-bucket, its position does not change.
                # If it was not in the bucket yet, then it is added as the most recently used in that bucket.
                # This _may_ kick out the node from above.
                Update k-buckets with all nodes in R
            If <nodeID> has been found, stop
        '''

        # target_id = request.idkey  # The ID we are trying to locate

        # if (target_id == self.node_id):
        #     response = csci4220_hw4_pb2.NodeList(
        #         responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
        #         nodes=closest_nodes
        #     )
        #     return response

        # # Sort the k-bucket by distance, which is determined by XOR
        # # sorted_nodes = sorted(self.k_bucket, key=lambda node: find_distance_between_nodes(node.id, target_id))

        # # Select k closest nodes
        # # closest_nodes = sorted_nodes[:self.k]
        # closest_nodes = list()
        # closest_nodes.append(csci4220_hw4_pb2.Node(id=5, port=self.port, address=self.address))
        # closest_nodes.append(csci4220_hw4_pb2.Node(id=6, port=self.port, address=self.address))

        # print("server id ", self.node_id)
        # response = csci4220_hw4_pb2.NodeList(
        #     responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
        #     nodes=closest_nodes
        # )
        # return response

# k_buckets: list of Nodes
def print_kbuckets(k_buckets):
    # <i> [<oldest entry> <next oldest entry> ... <newest entry>]<newline>
    # e.g. 3: 9:9002 10:9003
    for i in range(4):
        if (k_buckets[i] == [None]):
            print(f'{i}:')
        else:
            bucket = ' '.join(str(str(node.id) + ':' + str(node.port)) for node in k_buckets[i])
            print(f'{i}: {bucket}')

def start_server(local_id, kad_node, my_port):
    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csci4220_hw4_pb2_grpc.add_KadImplServicer_to_server(kad_node, server)

    # server.add_insecure_port(f'[::]:{my_port}')
    server.add_insecure_port(f'[::]:{my_port}')
    server.start()

    # Keep server running (to handle incoming requests)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"Node {local_id}: Shutting down server.")
        server.stop(0)

def find_distance_between_nodes(node1, node2):
    return abs(node1.id ^ node2.id)

# given distance between two nodes, find the right k-bucket index
# distance >= 0
def get_bucket_index(distance):
    if (distance <= 1):
        return 0
    elif (distance <= 3):
        return 1
    elif (distance <= 7):
        return 2
    else:
        return 3

# Function to handle commands from the standard input
def handle_commands(kad_node, local_id, my_address, my_port, k):
    while True:
        command_input = input("")
        command = command_input.strip().split(' ')
        if command[0] == "QUIT":
            print("Stopping the server...")
            break
        elif command[0] == "BOOTSTRAP": # BOOTSTRAP <remote hostname> <remote port>
            # IMPORTANT: when testing, replace all custom host names (e.g. peer01) with 127.0.1.1
            # for example, BOOTSTRAP 127.0.1.1 9000
            
            remote_hostname_input = command[1]
            remote_port_input = command[2]
            # print(remote_hostname_input, remote_port_input)
            remote_addr = socket.gethostbyname(remote_hostname_input)
            remote_port = int(remote_port_input)
            # print("remote ", remote_addr, remote_port)
            # print("my ", my_address, my_port)
            channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))
            # channel = grpc.insecure_channel(my_address + ':' + str(my_port))
            
            local_node = csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            request = csci4220_hw4_pb2.IDKey(
                node=local_node,
                idkey=local_id  # ID key to search for
            )
            
            try:
                # print("sending: ", request)
                response = stub.FindNode(request)
                # print("got a response", response)
                # response = stub.JoinNetwork(csci4220_hw4_pb2.NodeIDRequest(node_id=local_id))
                
                kad_node.update_k_buckets(response.responding_node)

                # response_node = KadImpl(response.responding_node.id, response.responding_node.address, response.responding_node.port, k)
                # kad_node.saveImpl(response.responding_node.id, response_node)

                print(f'After BOOTSTRAP({local_id}), k-buckets are:')
                print_kbuckets(kad_node.k_buckets)

            finally:
                channel.close()
            
        else:
            print(f"Unknown command: {command}")

def run():
    if len(sys.argv) != 4:
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)

    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
    k = int(sys.argv[3])
    my_hostname = socket.gethostname() # Gets my host name
    my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

    kad_node = KadImpl(local_id, my_address, my_port, k)
    
    server_thread = threading.Thread(target=start_server, daemon=True, args = [local_id, kad_node, my_port])
    server_thread.start()

    time.sleep(1) # wait for server to run...

    ''' Use the following code to convert a hostname to an IP and start a channel
    Note that every stub needs a channel attached to it
    When you are done with a channel you should call .close() on the channel.
    Submitty may kill your program if you have too many file descriptors open
    at the same time. '''

    handle_commands(kad_node, local_id, my_address, my_port, k)

    # # Keep server running (to handle incoming requests)
    # try:
    #     server.wait_for_termination()
    # except KeyboardInterrupt:
    #     print(f"Node {local_id}: Shutting down server.")
    #     server.stop(0)


if __name__ == '__main__':
    run()

