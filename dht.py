import json
import zmq

from node import Node, random_20bits
from table import RoutingTable


class DHT(object):
    def __init__(self, node, bootstrap=None):
        self.bootstrap = bootstrap
        self.node = node
        self.routing_table = RoutingTable(node, bootstrap)
        self.context = zmq.Context()
        self._sockets = {}

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
        for _ in xrange(3):
            for _, node in self.routing_table.find_closest(self.node):
                node_key = tuple(node.data)
                if node_key not in seen:
                    new_nodes = self.send_message_to_node(node, 'FIND_NODE', tuple(key.data))
                    for data, host, port in new_nodes:
                        new_node = Node(bytearray(data), host, port)
                        if new_node != self.node:
                            print "Discovered %s" % new_node
                            self.routing_table.update(new_node)
                    seen.add(node_key)
        print self.routing_table.buckets

    def run(self):
        socket = self.context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % self.node.port)
        if self.bootstrap:
            self.iterative_find_node(self.node)
        while True:
            message = json.loads(socket.recv())

            # Update routing table if message is from a new
            # sender
            req_node = Node(
                bytearray(message['from']['uid']),
                message['from']['host'],
                message['from']['port']
            )
            print "Received from %s" % req_node
            self.routing_table.update(req_node)
            if message['type'] == 'FIND_NODE':
                self.find_node_handler(message['value'], socket)
                print self.routing_table.buckets


def run(uid, port, boot, boot_port):
    node = Node(bytearray(uid), port=port)
    boot_node = None
    if boot and boot_port:
        boot_node = Node(bytearray(boot), port=boot_port)
    dht = DHT(node, boot_node)
    print list(node.data)
    dht.run()

def slave(uid):
    node = Node(random_20bits(), port=3001)
    bootstrap = Node(bytearray(uid), port=3000)
    dht = DHT(node, bootstrap)
    dht.run()


if __name__ == '__main__':
    m1 = [243, 103, 248, 223, 230, 45, 34, 177, 50, 180, 195, 195, 135, 146, 212, 158, 9, 55, 10, 94]
    m2 = [243, 106, 248, 223, 230, 230, 34, 177, 50, 180, 195, 195, 135, 146, 212, 158, 9, 55, 10, 94]
    m3 = [130, 106, 248, 223, 230, 230, 34, 177, 50, 180, 195, 195, 135, 146, 212, 158, 9, 55, 10, 94]
    m4 = [130, 220, 248, 223, 230, 230, 34, 177, 50, 180, 195, 195, 135, 146, 212, 158, 9, 55, 10, 94]

    import sys
    if sys.argv[1] == 'master':
        run(m1, 3000, boot=None, boot_port=None)
    elif sys.argv[1] == 'slave1':
        run(m2, 3001, boot=m1, boot_port=3000)
    elif sys.argv[1] == 'slave2':
        run(m3, 3002, boot=m1, boot_port=3000)
    elif sys.argv[1] == 'slave3':
        run(m4, 3003, boot=m2, boot_port=3001)
