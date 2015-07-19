import re
import random
import sys
import json
import zmq
from itertools import count
from flask import Flask

import threading
from node import Node, random_32bytes
from table import RoutingTable

import hashlib

c = zmq.Context()
ports = count(3000)
app = Flask(__name__)
kads = []

def distributed_hash(key):
    return bytearray(hashlib.md5(key).hexdigest())


class DHT(object):
    def __init__(self, node, bootstrap=None, context=None):
        self.data = {}
        self.bootstrap = bootstrap
        self.node = node
        self.routing_table = RoutingTable(node, bootstrap)
        self.context = context if context != None else zmq.Context()
        self._thread_uid = None

    def get_value_handler(self, data_key, key, sock):
        try:
            sock.send(json.dumps({
                'value': self.data[data_key]
            }))
        except KeyError:
            print "%s: GET_VALUE for %s failed. Trying nearest" % (self.node, data_key)
            self.find_node_handler(key, sock)

    def store_value_handler(self, key, value, sock):
        self.data[key] = value
        print "Stored '%s' on %s" % (key, self.node)
        sock.send(json.dumps({
            'result': 'OK'
        }))

    def find_node_handler(self, key, sock):
        response = []
        for _, node in self.routing_table.find_closest(Node(key)):
            response.append((tuple(node.data), node.host, node.port))
        sock.send(json.dumps({
            'nodes': response
        }))

    def send_message_to_node(self, node, message_type, message_value=None):
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

    def iterative_find_node(self, key, data_key=None):
        command = 'GET_VALUE' if data_key else 'FIND_NODE'

        # Build a set of seen items and a shortlist that
        # will be incremented
        seen = set()
        shortlist = self.routing_table.find_closest(key)
        best_score_so_far = float('inf')
        while len(shortlist) > 0 and len(shortlist) <= 20:

            # For each iteration, pick alpha contacts: a set
            # of K contacts that have not been seen
            alpha_contacts = [
                (score, node) for score, node in sorted(shortlist)
                if node not in seen
            ][:20]
            if len(alpha_contacts) == 0:
                break

            # Ensure that we are always improving our score over time
            best_score_currently = alpha_contacts[0][0]
            if best_score_currently >= best_score_so_far:
                break
            best_score_so_far = best_score_currently

            for _, node in alpha_contacts:
                seen.add(node)
                if data_key:
                    result = self.send_message_to_node(node, 'GET_VALUE', {
                        'data_key': data_key,
                        'key': tuple(key.data)
                    })
                    if 'value' in result:
                        return result['value'], []
                    new_nodes = result['nodes']
                else:
                    new_nodes = self.send_message_to_node(node, 'FIND_NODE', tuple(key.data))['nodes']

                for data, host, port in new_nodes:
                    new_node = Node(bytearray(data), host, port)
                    if new_node == self.node:
                        continue

                    # Update routing table and shortlist.
                    self.routing_table.update(new_node)
                    shortlist.append(((key ^ new_node).distance_key(), new_node))

        return None, [node for _, node in sorted(shortlist)][:20]


    def run(self):
        self._thread_uid = threading.Thread(target=self._run)
        self._thread_uid.daemon = True
        self._thread_uid.start()
        if self.bootstrap:
            self.iterative_find_node(self.node)
        return self._thread_uid

    def __setitem__(self, key, value):
        hashed_key = distributed_hash(key)
        search_node = Node(hashed_key)
        result = self.routing_table.find_closest(search_node)
        if len(result) == 0:
            self.data[key] = value
        else:
            for _, node in result:
                self.send_message_to_node(node, 'STORE_VALUE', {
                    'key': key,
                    'value': value
                })

    def __getitem__(self, key):
        hashed_key = distributed_hash(key)
        try:
            return self.data[tuple(hashed_key)]
        except KeyError:
            search_node = Node(hashed_key)
            found, _ = self.iterative_find_node(search_node, key)
            if found:
                return found
            raise KeyError()

    def _run(self):
        socket = self.context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % self.node.port)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        while True:
            found = poller.poll(2000)
            if len(found) == 0:
                nodes_known = [len(b) for b in self.routing_table.buckets]
                #print "%s knows %s peers" % (self.node, sum(nodes_known))
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
            elif message['type'] == 'STORE_VALUE':
                self.store_value_handler(
                    message['value']['key'],
                    message['value']['value'],
                    socket
                )
            elif message['type'] == 'GET_VALUE':
                self.get_value_handler(
                    message['value']['data_key'],
                    message['value']['key'],
                    socket
                )


def run(uid, port, boot, boot_port):
    node = Node(bytearray(uid), port=port)
    boot_node = None
    if boot and boot_port:
        boot_node = Node(bytearray(boot), port=boot_port)
    dht = DHT(node, boot_node)
    dht.run()

def slave(uid):
    node = Node(random_32bytes(), port=3001)
    bootstrap = Node(bytearray(uid), port=3000)
    dht = DHT(node, bootstrap)
    dht.run()


@app.route('/<int:id>/data')
def get_data(id):
    return json.dumps(kads[id].data)

@app.route('/<int:id>/<key>')
def get_key(id, key):
    return json.dumps({
        'data': kads[id][key]
    })

@app.route('/<int:id>/<key>/<value>')
def set_key(id, key, value):
    kads[id][key] = value
    return 200, ""

@app.route('/create')
def create():
    node = Node(random_32bytes(), port=next(ports))
    import ipdb; ipdb.set_trace()
    seed = random.choice(kads).node
    dht = DHT(node, seed, context=c)
    kads.append(dht)
    processes.append(dht.run())
    return json.dumps({
        'n': len(processes)-1
    })

if __name__ == '__main__':
    processes = []
    n_processes = int(sys.argv[1])
    for _ in xrange(n_processes):
        try:
            seed = random.choice(kads).node
        except IndexError:
            seed = None

        print "STARTED"
        node = Node(random_32bytes(), port=next(ports))
        dht = DHT(node, seed, context=c)
        kads.append(dht)

        processes.append(dht.run())

    from time import sleep
    n_failures = 0
    with open('lipsum.txt') as f:
        result = re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+", f.read())
        sleep(1)
        for idx in xrange(1, len(result)):
            first, second = result[idx-1], result[idx]
            random.choice(kads)[first] = second
        sleep(1)
        for idx in xrange(1, len(result)):
            first, second = result[idx-1], result[idx]
            try:
                print "%s -> %s" % (first, random.choice(kads)[first])
            except KeyError:
                n_failures += 1
    print "Failed %s times" % n_failures
    
    app.run()
