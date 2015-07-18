import itertools
import random
import unittest


def random_32bytes():
    return bytearray([random.randint(0, 255) for _ in xrange(32)])

class Node(object):
    def __init__(self, data, host='localhost', port=3000):
        assert len(data) == 32
        self.host = host
        self.port = port
        self.data = data

    def __xor__(self, other):
        return Node(bytearray([bit_a ^ bit_b for bit_a, bit_b in zip(self.data, other.data)]))

    def __eq__(self, other):
        return tuple(other.data) == tuple(self.data)

    def __ne__(self, other):
        return tuple(other.data) != tuple(self.data)

    def __hash__(self):
        return hash(tuple(self.data))

    def distance_key(self):
        res = 0 
        for bit_n, byte in enumerate(self.data):
            for i in xrange(7, -1, -1):
                if byte & (1 << i) != 0:
                    return bit_n * 8 + (7 - i)

        return len(self.data) * 7

    def __repr__(self):
        return ''.join(map(str, list(self.data)))


class NodeTestCase(unittest.TestCase):

    def test_it_exists(self):
        n = Node(bytearray(['a'] * 32))
        self.assertEqual(list(n.data), list(itertools.repeat(97, 32)))

    def test_random_node_creation_works(self):
        n = Node(random_32bytes())
        self.assertEqual(len(n.data), 32)

    def test_xor_distance(self):
        n1 = Node(bytearray(['a'] * 32))
        n2 = Node(bytearray(['b'] * 32))
        result = n1 ^ n2
        self.assertEqual(list(result.data), list(itertools.repeat(3, 32)))

    def test_distance_key(self):
        payload = [0] * 5 + [(2**5)-1] + [255] * 26
        n = Node(bytearray(payload))
        self.assertEqual(n.distance_key(), (8 * 5) + 3)

        payload = [0] * 32
        n = Node(bytearray(payload))
        self.assertEqual(n.distance_key(), 32 * 7)

    def test_equalness(self):
        self.assertEqual(
            Node(bytearray(['a'] * 32)),
            Node(bytearray(['a'] * 32)),
        )


if __name__ == '__main__':
    unittest.main()
