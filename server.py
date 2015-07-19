import re
import json
import random
import argparse
from node import Node, random_32bytes
from table import RoutingTable
from dht import DHT
from time import sleep
import zmq

from flask import Flask
from itertools import count

app = Flask(__name__)
context = zmq.Context()
ports = count(3000)
processes = []
kads = []

@app.route('/<int:id>/data')
def get_data(id):
    try:
        return json.dumps(kads[id].data)
    except KeyError:
        return "", 404

@app.route('/<int:id>/<key>')
def get_key(id, key):
    try:
        return json.dumps({
            'data': kads[id][key]
        })
    except KeyError:
        return "", 404

@app.route('/<int:id>/<key>/<value>')
def set_key(id, key, value):
    try:
        kads[id][key] = value
        return "", 202
    except KeyError:
        return "", 404

@app.route('/kill/<int:n>')
def kill(n):
    for k in kads[n:]:
        k.shutdown()

    del kads[n:]

@app.route('/create')
def create():
    node = Node(random_32bytes(), port=next(ports))
    seed = random.choice(kads).node
    dht = DHT(node, seed, context=context)
    kads.append(dht)
    processes.append(dht.run())
    return json.dumps({
        'n': len(processes)-1
    })

def main(n_processes, fill, test, needs_server):
    for _ in xrange(n_processes):
        try:
            seed = random.choice(kads).node
        except IndexError:
            seed = None

        node = Node(random_32bytes(), port=next(ports))
        dht = DHT(node, seed, context=context)
        kads.append(dht)
        processes.append(dht.run())
        print "%s started." % node

    if fill:
        with open('./lipsum.txt') as f:
            result = re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+", f.read())
            for idx in xrange(1, len(result)):
                first, second = result[idx-1], result[idx]
                random.choice(kads)[first] = second
            if test:
                n_failures = 0
                for idx in xrange(1, len(result)):
                    first, second = result[idx-1], result[idx]
                    try:
                        print "%s -> %s" % (first, random.choice(kads)[first])
                    except KeyError:
                        n_failures += 1
                print "Failed %s times" % n_failures
    if needs_server:
        app.run()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start N kad servers')
    parser.add_argument('N', type=int, default=10, help='number of server to create')
    parser.add_argument('--fill', default=False, action='store_true', help='fill with dummy data')
    parser.add_argument('--test', default=False, action='store_true', help='test filled data. Only works with test')
    parser.add_argument('--server', default=False, action='store_true', help='spins up a REST server')
    args = parser.parse_args()
    print args
    main(args.N, args.fill, args.test, args.server)
