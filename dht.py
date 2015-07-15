import random
import sys
import json
import zmq
from itertools import count

import threading
from node import Node, random_20bits
from table import RoutingTable


class DHT(object):
    def __init__(self, node, bootstrap=None):
        self.bootstrap = bootstrap
        self.node = node
        self.routing_table = RoutingTable(node, bootstrap)
        self.context = zmq.Context()
        self._sockets = {}
        self._thread_uid = None

    @property
    def started(self):
        return self._thread_uid != None

    def find_node_handler(self, key, sock):
        response = []
        for _, node in self.routing_table.find_closest(Node(key)):
            response.append((tuple(node.data), node.host, node.port))
        sock.send(json.dumps(response))

    def send_message_to_node(self, node, message_type, message_value=None):
        try:
            socket = self._sockets[tuple(node.data)]
        except KeyError:
            socket = self.context.socket(zmq.REQ)
            socket.connect("tcp://%s:%s" % (node.host, node.port))

        socket.send(json.dumps({
            'from': {
                'uid': tuple(self.node.data),
                'host': self.node.host,
                'port': self.node.port,
            },
            'type': message_type,
            'value': message_value
        }))
        return json.loads(socket.recv())

    def iterative_find_node(self, key):
        seen = set()
        while True:
            closest_nodes = self.routing_table.find_closest(self.node)
            processed = set()
            for _, node in closest_nodes:
                node_key = tuple(node.data)
                if node_key not in seen:
                    new_nodes = self.send_message_to_node(node, 'FIND_NODE', tuple(key.data))
                    for data, host, port in new_nodes:
                        new_node = Node(bytearray(data), host, port)
                        if new_node != self.node:
                            print "Discovered %s" % new_node
                            processed.add(tuple(new_node.data))
                            self.routing_table.update(new_node)
                    seen.add(node_key)

            if len(seen) >= 20 or len(processed) == 0:
                print "BREAKING"
                break
        print "FINISHED ITERATIVE FIND NODE FOR %s" % self.node

    def run(self):
        self._thread_uid = threading.Thread(target=self._run)
        self._thread_uid.daemon = True
        self._thread_uid.start()
        if self.bootstrap:
            self.iterative_find_node(self.node)
        return self._thread_uid

    def _run(self):
        socket = self.context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % self.node.port)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        while True:
            found = poller.poll(2000)
            if len(found) == 0:
                nodes_known = [len(b) for b in self.routing_table.buckets]
                print "%s knows %s peers" % (self.node, sum(nodes_known))
                continue

            message = json.loads(socket.recv())

            # Update routing table if message is from a new
            # sender
            req_node = Node(
                bytearray(message['from']['uid']),
                message['from']['host'],
                message['from']['port']
            )
            #print "Received from %s" % req_node
            self.routing_table.update(req_node)
            if message['type'] == 'FIND_NODE':
                self.find_node_handler(message['value'], socket)
                #print self.routing_table.buckets


def run(uid, port, boot, boot_port):
    node = Node(bytearray(uid), port=port)
    boot_node = None
    if boot and boot_port:
        boot_node = Node(bytearray(boot), port=boot_port)
    dht = DHT(node, boot_node)
    dht.run()

def slave(uid):
    node = Node(random_20bits(), port=3001)
    bootstrap = Node(bytearray(uid), port=3000)
    dht = DHT(node, bootstrap)
    dht.run()


if __name__ == '__main__':
    ports = count(3000)
    processes = []
    kads = []
    n_processes = int(sys.argv[1])
    for _ in xrange(n_processes):
        try:
            seed = random.choice(kads).node
        except IndexError:
            seed = None

        node = Node(random_20bits(), port=next(ports))
        dht = DHT(node, seed)
        kads.append(dht)

        #from time import sleep
        #sleep(0.1)
        processes.append(dht.run())

    [p.join() for p in processes]
