import itertools
import random
import unittest
from collections import deque

from node import Node, random_32bytes


class RoutingTable(object):
    def __init__(self, node, bootstrap=None):
        self.node = node
        self.buckets = [deque() for _ in xrange(32 * 8)]
        if bootstrap:
            self.update(bootstrap)

    def update(self, node):
        bucket_n = (self.node ^ node).distance_key()
        if node not in self.buckets[bucket_n]:
            self.buckets[bucket_n].append(node)

    def find_closest(self, target_node, n=20):
        starting_bucket_idx = (self.node ^ target_node).distance_key()
        return self._find_closest_bucket(starting_bucket_idx, target_node, n)

    def _find_closest_bucket(self, starting_bucket_idx, target_node, n=20):
        shift = 0
        result = []
        finished_left = finished_right = False
        while finished_left == False or finished_right == False:
            if len(result) >= n:
                break

            # Handle left
            left_idx = starting_bucket_idx - shift
            finished_left = left_idx < 0
            if not finished_left:
                result.extend([
                    ((target_node ^ n).distance_key(), n)
                    for n in self.buckets[left_idx]
                ])

            # Handle right
            right_idx = starting_bucket_idx + shift
            finished_right = right_idx >= len(self.buckets)
            if not finished_right and shift > 0:
                result.extend([
                    ((target_node ^ n).distance_key(), n)
                    for n in self.buckets[right_idx]
                ])
            shift += 1
        
        return sorted(result)[:20]


class TableTestCase(unittest.TestCase):
    def setUp(self):
        self.node = Node(bytearray(['a'] * 20))

    def test_table_exists(self):
        table = RoutingTable(self.node)
        self.assertTrue(table)
        self.assertEqual(len(table.buckets), 20 * 8)

    def test_update_node(self):
        table = RoutingTable(self.node)
        table.update(Node(bytearray(['b'] * 20)))
        self.assertEqual(len(table.buckets[6]), 1)

    def test_find_closest(self):
        nodes = [Node(random_20bits()) for _ in xrange(6)]
        table = RoutingTable(self.node)
        table.buckets[4] = nodes[:2]
        table.buckets[3] = [nodes[2]]
        table.buckets[5] = nodes[3:5]
        table.buckets[10] = [nodes[5]]

        search_node = Node(random_20bits())
        result = table._find_closest_bucket(4, search_node)
        self.assertEqual(result, sorted([
            ((search_node ^ n).distance_key(), n) for n in nodes
        ]))


if __name__ == '__main__':
    unittest.main()
