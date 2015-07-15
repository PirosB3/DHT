import itertools
import random
import unittest


def random_20bits():
    return bytearray([random.randint(0, 255) for _ in xrange(20)])

class Node(object):
    def __init__(self, data, host='localhost', port=3000):
        assert len(data) == 20
        self.host = host
        self.port = port
        self.data = data

    def __xor__(self, other):
        return Node(bytearray([bit_a ^ bit_b for bit_a, bit_b in zip(self.data, other.data)]))

    def __eq__(self, other):
        return tuple(other.data) == tuple(self.data)

    def __ne__(self, other):
        return tuple(other.data) != tuple(self.data)

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
        n = Node(bytearray(['a'] * 20))
        self.assertEqual(list(n.data), list(itertools.repeat(97, 20)))

    def test_random_node_creation_works(self):
        n = Node(random_20bits())
        self.assertEqual(len(n.data), 20)

    def test_xor_distance(self):
        n1 = Node(bytearray(['a'] * 20))
        n2 = Node(bytearray(['b'] * 20))
        result = n1 ^ n2
        self.assertEqual(list(result.data), list(itertools.repeat(3, 20)))

    def test_distance_key(self):
        payload = [0] * 5 + [(2**5)-1] + [255] * 14
        n = Node(bytearray(payload))
        self.assertEqual(n.distance_key(), (8 * 5) + 3)

        payload = [0] * 20
        n = Node(bytearray(payload))
        self.assertEqual(n.distance_key(), 20 * 8)

    def test_equalness(self):
        self.assertEqual(
            Node(bytearray(['a'] * 20)),
            Node(bytearray(['a'] * 20)),
        )


if __name__ == '__main__':
    unittest.main()
