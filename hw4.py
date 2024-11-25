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
        self.node_id = int(node_id)
        self.address = str(local_address)
        self.port = int(local_port)
        self.node_classes = dict() # maps node id to it kade node implementation (KadImpl)
        self.k_buckets = [[] for i in range(4)] # list of nodes, separated by distance
        self.node = csci4220_hw4_pb2.Node(id=int(node_id), port=int(local_port), address=local_address)
        self.k = k
        self.key_vals = dict()
        self.known_nodes = list() # nodes we know; contains every node that was once stored in k_buckets (including the one that got removed)

    # def add_to_kbucket(self, retrieved_node):
    #     distance = find_distance_between_nodes(self.node, retrieved_node)
    #     k_idx = get_bucket_index(distance)
    #     self.k_buckets[k_idx].append(retrieved_node)
    
    # remote node has notified us it is quitting; remove that remote node
    def remove_from_kbucket(self, node_to_remove):
        distance = find_distance_between_nodes(self.node, node_to_remove)
        k_idx = get_bucket_index(distance)

        # check if node is in k-buckets
        if (node_to_remove not in self.k_buckets[k_idx]): 
            print(f'No record of quitting node {node_to_remove.id} in k-buckets.')
            return
        
        print(f'Evicting quitting node {node_to_remove.id} from bucket {k_idx}')
        self.k_buckets[k_idx].remove(node_to_remove)

    # return a list that removed duplicate nodes from node_list
    def remove_duplicate_nodes(self, node_list):
        result = []
        for element in node_list:
            if element not in result:
                result.append(element)
        return result

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

        # If a node was already in a k-bucket, its position does not change.
        # if (node in bucket):
        if (node in bucket):
            return
        elif len(bucket) >= self.k:
            # Bucket is full, remove the least recently used (first) and add new node
            bucket.pop(0)
        # If it was not in the bucket yet, then it is added as the most recently used in that bucket.
        bucket.append(node)

    # send findNode request to another node we know
    # returns the k closest nodes returned by that node
    def send_FindNode_request(self, requester, target_id, address, port):
        channel = grpc.insecure_channel(address + ':' + str(port))
        
        # local_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
        request = csci4220_hw4_pb2.IDKey(
            node=requester,
            idkey=target_id
        )
        response = stub.FindNode(request)
        channel.close()

        # # parse the returned response 
        # node_list = list()
        # # Update k-buckets with all nodes in the response
        # for response_node in response.nodes:
        #     # If a node in R was already in a k-bucket, its position does not change.
        #     if (response_node.id != self.node_id):
        #         node = csci4220_hw4_pb2.Node(id=response_node.id, port=response_node.port, address=response_node.address)
        #         node_list.append(node)

        # # print(node_list)
        return response.nodes
    
    # send findValue request to another node we know
    # returns the k closest nodes returned by that node
    def send_FindValue_request(self, requester, target_key, address, port):
        channel = grpc.insecure_channel(address + ':' + str(port))
        
        # local_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
        request = csci4220_hw4_pb2.IDKey(
            node=requester,
            idkey=target_key  
        )
        response = stub.FindValue(request)
        channel.close()
        return response.mode_kv, response.kv, response.nodes
    
    # send findValue request to another node we know
    # returns the k closest nodes returned by that node
    def send_Store_request(self, key, val, requester_node, address, port):
        channel = grpc.insecure_channel(address + ':' + str(port))
        
        # local_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
        request = csci4220_hw4_pb2.KeyValue(
            node=requester_node,    # original requester's node
            key = key,
            value = val 
        )
        stub.Store(request)
        channel.close()

    # send findValue request to another node we know
    # returns the k closest nodes returned by that node
    # def send_getVal_request(self, key, address, port):
    #     channel = grpc.insecure_channel(address + ':' + str(port))
        
    #     # local_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
    #     stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
    #     # request = csci4220_hw4_pb2.KeyValue(
    #     #     node=requester_node,    # original requester's node
    #     #     key = key,
    #     #     value = val 
    #     # )
    #     channel.close()
    #     return stub.storeKeyVal(key)
    
    # send findValue request to another node we know
    # returns the k closest nodes returned by that node
    def send_handleQuit_request(self, address, port):
        channel = grpc.insecure_channel(address + ':' + str(port))
        
        # local_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
        request = csci4220_hw4_pb2.IDKey(
            node=self.node,    
            idkey = 1   # this value can be arbitrary
        )
        
        stub.Quit(request)
        channel.close()
    
    # given a key value pair, store it in this node
    def storeKeyVal(self, request):
        self.key_vals[request.key] = request.value

    def getVal(self, request):
        return self.key_vals[request.key]
    
    # def handleQuit(self, request):
    #     self.remove_from_kbucket(request.node)

    def FindNode(self, request, context):
        # print(request)
        # if (request.node.id != self.node_id):
        #     print(f'Serving FindNode({request.idkey}) request for {request.node.id}')
        #     # self.update_k_buckets(request.node)
        
        # this node is what the requester is lookin for
        if (request.idkey == self.node_id): # or request.idkey == request.node.id
            known_nodes = [node for nodes in self.k_buckets for node in nodes]
            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, request.idkey))[:self.k]

            node_to_return = csci4220_hw4_pb2.Node(id=self.node_id,port=self.port,address=self.address)
            response = csci4220_hw4_pb2.NodeList(
                responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
                nodes=k_closest_nodes
            )

            if (request.node.id != self.node_id):
                self.update_k_buckets(request.node)
                self.known_nodes.append(request.node)

            return response
        
        if (request.node.id != self.node_id):
            print(f'Serving FindNode({request.idkey}) request for {request.node.id}')

        # request is looking for itself (for BOOTSTRAP)
        if (request.idkey == request.node.id):
            known_nodes = [node for nodes in self.k_buckets for node in nodes]
            # known_nodes.append(self.node)

            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, request.idkey))[:self.k]
            # nodes_to_return = list()
            # for node in k_closest_nodes:
            #     node_to_return = csci4220_hw4_pb2.Node(id=node.id,port=node.port,address=node.address)
            #     nodes_to_return.append(node_to_return)
            
            response = csci4220_hw4_pb2.NodeList(
                responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
                nodes=k_closest_nodes
            )

            if (request.node.id != self.node_id):
                self.update_k_buckets(request.node)
                self.known_nodes.append(request.node)

            return response

        
        # if another node has sent this request, send k closest node back to it
        if (request.node.id != self.node_id):
            known_nodes = [node for nodes in self.k_buckets for node in nodes]
            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, request.idkey))[:self.k]
            
            response = csci4220_hw4_pb2.NodeList(
                responding_node=self.node,
                nodes=k_closest_nodes
            )

            self.update_k_buckets(request.node)
            self.known_nodes.append(request.node)
            return response

        target_id = request.idkey  # The ID we are trying to locate
        known_nodes = [node for nodes in self.k_buckets for node in nodes]
        # visited = [request.node]
        visited = [request.node]
        node_found = False
        # sorted(known_nodes, key=lambda n: find_distance_between_nodes(target_id, n))[:self.k]
        # closest_nodes = deque(self.get_k_closest(node_id, known_nodes), maxlen=self.k)
        # closest_nodes = [node for nodes in self.k_buckets for node in nodes if node not in visited]

        # get k closest IDs to nodeID
        k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, target_id))[:self.k]
        while (any(node not in visited for node in k_closest_nodes) and not node_found):
            # get nodes we've not visited
            nodes_to_visit = [node for node in k_closest_nodes if node not in visited]
            # Sort the k-bucket by distance, which is determined by XOR
            # closest_node = sorted(self.nodes, key=lambda node: find_distance_between_nodes(node.id, target_id))
            # print("nodes to visit: ", nodes_to_visit)

            print("to visit", nodes_to_visit)
            for node in nodes_to_visit:
                visited.append(node)

                response_nodes = self.send_FindNode_request(request.node, request.idkey, node.address, node.port)
                print("response ", response_nodes)

                self.update_k_buckets(node)
                self.known_nodes.append(node)

                print("before: ")
                print_kbuckets(self.k_buckets)

                # Update k-buckets with all nodes in the response
                for response_node in response_nodes:
                    # If a node in R was already in a k-bucket, its position does not change.
                    if (response_node.id != self.node_id):
                        self.update_k_buckets_no_removal(response_node)
                        # if (response_node not in self.known_nodes):
                        #     known_nodes.append(response_node)

                print("after: ")
                print_kbuckets(self.k_buckets)

                # concatenate list
                # known_nodes.extend(response_nodes)  

                # # remove duplicates
                # known_nodes = self.remove_duplicate_nodes(known_nodes)  

                #update known nodes
                known_nodes = [node for nodes in self.k_buckets for node in nodes]

                # for node in known_nodes:
                #     if (node.id == target_id):
                #         node_found = True
                #         break 
                # if node_found:
                #     break               

            # get k closest IDs to nodeID
            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, target_id))[:self.k]

            for node in k_closest_nodes:
                if (node.id == target_id):
                    node_found = True
                    break 
        
        if (request.node.id != self.node_id):
            self.update_k_buckets(request.node)
            self.known_nodes.append(request.node)

        nodes_to_return = list()
        for node in k_closest_nodes:
            node_to_return = csci4220_hw4_pb2.Node(id=node.id,port=node.port,address=node.address)
            nodes_to_return.append(node_to_return)

        response = csci4220_hw4_pb2.NodeList(
            responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
            nodes=nodes_to_return
        )
        return response

    def FindValue(self, request, context):
        # print(request)
        if (request.node.id != self.node_id):
            print(f'Serving FindKey({request.idkey}) request for {request.node.id}')
            # self.update_k_buckets(request.node)

        target_key = request.idkey  # The key we are trying to locate

        known_nodes = [node for nodes in self.k_buckets for node in nodes]
        visited = [request.node]
        key_found = False

        # if this node has the key, return its value
        # b = target_key in self.key_vals.keys()
        # print(target_key, self.key_vals.keys(), b)
        if target_key in self.key_vals.keys():
            if (request.node.id != self.node_id):
                self.update_k_buckets(request.node)
                self.known_nodes.append(request.node)

            if (request.node.id == self.node_id):
                print(f'Found data "{self.key_vals[target_key]}" for key {target_key}')
            node_to_return = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
            key_val = csci4220_hw4_pb2.KeyValue(node=node_to_return, key=target_key, value=self.key_vals[target_key])
            response = csci4220_hw4_pb2.KV_Node_Wrapper(
                responding_node=node_to_return,
                mode_kv = True,
                kv=key_val,
                nodes=[node_to_return]  # arbitrary
            )
            return response

        # if another node has sent this request, send message that there's no key associated with this
        if (request.node.id != self.node_id):
            self.update_k_buckets(request.node)
            self.known_nodes.append(request.node)

            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, target_key))[:self.k]
            node_to_return = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
            key_val = csci4220_hw4_pb2.KeyValue(node=node_to_return, key=target_key, value="")
            response = csci4220_hw4_pb2.KV_Node_Wrapper(
                responding_node=node_to_return,
                mode_kv = False,
                kv=key_val, # arbitrary
                nodes=k_closest_nodes
            )
            return response

        key_value = None
        response_nodes = None

        # get k closest IDs to nodeID
        k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, target_key))[:self.k]
        while (any(node not in visited for node in k_closest_nodes) and not key_found):
            # get nodes we've not visited
            nodes_to_visit = [node for node in k_closest_nodes if node not in visited]
            for node in nodes_to_visit:
                visited.append(node)

                key_found, key_value, response_nodes = self.send_FindValue_request(request.node, target_key, node.address, node.port)
                # if (node.id != self.node_id):
                self.update_k_buckets(node)
                self.known_nodes.append(node)

                # Update k-buckets with all nodes in the response
                # if not key_found:
                for response_node in response_nodes:
                    # If a node in R was already in a k-bucket, its position does not change.
                    if (response_node.id != self.node_id):
                        # node = csci4220_hw4_pb2.Node(id=response_node.id, port=response_node.port, address=response_node.address)
                        self.update_k_buckets_no_removal(response_node)
                        # if (response_node not in self.known_nodes):
                            # print(self.known_nodes[0].id, response_node.id)
                            # known_nodes.append(response_node)

                # concatenate list
                # known_nodes.extend(response_nodes) 

                # remove duplicates
                # known_nodes = self.remove_duplicate_nodes(known_nodes)   
                
                # upate known_nodes
                known_nodes = [node for nodes in self.k_buckets for node in nodes]

                if (key_found):
                    break        

            # get k closest IDs to nodeID
            k_closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, target_key))[:self.k]

        # node with key found; return that node
        if key_found:
            # returned_val = self.send_getVal_request(target_key, node_with_key.address, node_with_key.port)
            if (request.node.id == self.node_id):
                print(f'Found value "{key_value.value}" for key {target_key}')

            known_nodes = [node for nodes in self.k_buckets for node in nodes]
            nodes_to_return = list()
            for node in known_nodes: # for node in k_closest_nodes
                node_to_return = csci4220_hw4_pb2.Node(id=node.id,port=node.port,address=node.address)
                nodes_to_return.append(node_to_return)

            response = csci4220_hw4_pb2.KV_Node_Wrapper(
                responding_node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
                mode_kv = True,
                kv=key_value,
                nodes=nodes_to_return
            )

            if (request.node.id != self.node_id):
                self.update_k_buckets(request.node)
                self.known_nodes.append(request.node)

            return response

        # node with key not found; return k closest nodes
        if (request.node.id == self.node_id):
            print(f'Could not find key {target_key}')

        # nodes_to_return = list()
        # for node in k_closest_nodes:
        #     node_to_return = csci4220_hw4_pb2.Node(id=node.id,port=node.port,address=node.address)
        #     nodes_to_return.append(node_to_return)

        responding_node = csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address)
        kv = csci4220_hw4_pb2.KeyValue(node=responding_node, key=target_key, value="")
        response = csci4220_hw4_pb2.KV_Node_Wrapper(
            responding_node=responding_node,
            mode_kv = False,
            kv=kv,       # arbitrary
            nodes=k_closest_nodes
        )

        if (request.node.id != self.node_id):
            self.update_k_buckets(request.node)
            self.known_nodes.append(request.node)
        return response
    
    # Store the value at the given node. 
    def Store(self, request, context):
        # client doesn't use return value; so return any IDKey object
        response = csci4220_hw4_pb2.IDKey(
            node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address),
            idkey=self.node_id 
        )

        # this response is from a different node; store the key
        if (request.node.id != self.node_id):
            print(f'Storing key {request.key} value "{request.value}"')
            self.storeKeyVal(request)
            self.update_k_buckets(request.node)
            self.known_nodes.append(request.node)
            return response
        
        # if (request.node_id != self.node_id):
        # self.update_k_buckets(request.node)

        # otherwise, we're sending the store request from this node

        # get the entire list of nodes we know, including current node as it may be the closest node
        known_nodes = [node for nodes in self.k_buckets for node in nodes]
        known_nodes.append(self.node)

        # find closest node and send key-value pair to that node
        closest_nodes = sorted(known_nodes, key=lambda n: find_distance_between_node_and_idkey(n, request.key))
        closest_node = closest_nodes[0]

        # current node is the closest node
        if (closest_node.id == self.node.id):
            print(f'Storing key {request.key} at node {closest_node.id}')
            # print(f'Storing key {request.key} value "{request.value}"')
            self.storeKeyVal(request)
            return response

        # current node is not the closest node
        print(f"Storing key {request.key} at node {closest_node.id}")
        self.send_Store_request(request.key, request.value, request.node, closest_node.address, closest_node.port)
        return response
    
    # Notify a remote node that the node with ID in IDKey is quitting the
    # network and should be removed from the remote node's k-buckets. 
    def Quit(self, request, context):
        # client doesn't use return value; so return any IDKey object
        response = csci4220_hw4_pb2.IDKey(
            node=csci4220_hw4_pb2.Node(id=self.node_id, port=self.port, address=self.address), # arbitrary
            idkey=self.node_id # arbitrary
        )

        # received quit request from another node. remove that node from our k-buckets
        if (request.node.id != self.node_id):
            self.remove_from_kbucket(request.node)
            return response

        # this node is trying to shut down. notify other nodes
        known_nodes = [node for nodes in self.k_buckets for node in nodes]
        for node in known_nodes:
            print(f"Letting {node.id} know I'm quitting.")
            self.send_handleQuit_request(node.address, node.port)

        print(f'Shut down node {self.node.id}')
        return response

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

def find_distance_between_node_and_idkey(node1, key):
    return abs(node1.id ^ key)

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
            channel = grpc.insecure_channel(kad_node.address + ':' + str(kad_node.port))

            local_node = csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            request = csci4220_hw4_pb2.IDKey(
                node=local_node,
                idkey=local_id    # arbitrary
            )

            try:
                response = stub.Quit(request)
            finally:
                channel.close()
                break
        elif command[0] == "BOOTSTRAP": # BOOTSTRAP <remote hostname> <remote port>
            # IMPORTANT: when testing, replace all custom host names (e.g. peer01) with 127.0.1.1
            # for example, BOOTSTRAP 127.0.1.1 9000
            
            remote_hostname_input = command[1]
            # remote_hostname_input = "127.0.0.1"
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
                idkey=local_id
            )
            
            try:
                # print("sending: ", request)
                response = stub.FindNode(request)

                kad_node.update_k_buckets(response.responding_node)

                known_nodes = response.nodes
                for n in known_nodes:
                    if (n.id != local_id):
                        # node = csci4220_hw4_pb2.Node(id=n.id, port=n.port, address=n.address)
                        kad_node.update_k_buckets(n)
                        kad_node.known_nodes.append(n)

                # print("got a response", response)
                # response = stub.JoinNetwork(csci4220_hw4_pb2.NodeIDRequest(node_id=local_id))
                
                # kad_node.update_k_buckets(response.responding_node)

                # response_node = KadImpl(response.responding_node.id, response.responding_node.address, response.responding_node.port, k)
                # kad_node.saveImpl(response.responding_node.id, response_node)

                print(f'After BOOTSTRAP({response.responding_node.id}), k-buckets are:')
                print_kbuckets(kad_node.k_buckets)

            finally:
                channel.close()
        elif command[0] == "FIND_NODE":
            target_node_id = int(command[1])
            channel = grpc.insecure_channel(kad_node.address + ':' + str(kad_node.port))

            # target_node = csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            request = csci4220_hw4_pb2.IDKey(
                node=kad_node.node,
                idkey=target_node_id    
            )

            try:
                print(f'Before FIND_NODE command, k-buckets are:')
                print_kbuckets(kad_node.k_buckets)

                response = stub.FindNode(request)

                if (len(response.nodes) > 0 and response.nodes[0].id == target_node_id):
                    print(f'Found destination id {target_node_id}')
                else:
                    # save k-closest node
                    print(f'Could not find destination id {target_node_id}')

                # known_nodes = response.nodes
                # for n in known_nodes:
                #     if (n.id != local_id):
                #         # node = csci4220_hw4_pb2.Node(id=n.id, port=n.port, address=n.address)
                #         kad_node.update_k_buckets(n)
                #         kad_node.known_nodes.append(request.node)

                print(f'After FIND_NODE command, k-buckets are:')
                print_kbuckets(kad_node.k_buckets)

            finally:
                channel.close()
        elif command[0] == "FIND_VALUE":
            target_key = int(command[1])
            channel = grpc.insecure_channel(kad_node.address + ':' + str(kad_node.port))

            local_node = csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            request = csci4220_hw4_pb2.IDKey(
                node=local_node,
                idkey=target_key    
            )

            try:
                print(f'Before FIND_VALUE command, k-buckets are:')
                print_kbuckets(kad_node.k_buckets)

                response = stub.FindValue(request)
                
                # if (response.mode_kv == False):
                # save k-closest node

                # if (not response.mode_kv):
                #     known_nodes = response.nodes
                #     for n in known_nodes:
                #         if (n.id != local_id):
                #             # node = csci4220_hw4_pb2.Node(id=n.id, port=n.port, address=n.address)
                #             kad_node.update_k_buckets(n)
                #             kad_node.known_nodes.append(request.node)

                print(f'After FIND_VALUE command, k-buckets are:')
                print_kbuckets(kad_node.k_buckets)

            finally:
                channel.close()
        elif command[0] == "STORE":
            key = int(command[1])
            value = command[2]
            
            channel = grpc.insecure_channel(kad_node.address + ':' + str(kad_node.port))

            local_node = csci4220_hw4_pb2.Node(id=local_id, port=int(my_port), address=my_address)
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            request = csci4220_hw4_pb2.KeyValue(
                node=kad_node.node,  
                key=key,
                value=value   
            )

            try:
                response = stub.Store(request)
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

