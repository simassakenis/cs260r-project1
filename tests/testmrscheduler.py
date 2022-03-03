import unittest
from simulator.nodes import LogicalNode, PhysicalNode, Input, MapNode, ReduceNode, ShuffleNode
from simulator.simulator import simulate

class TestMRScheduler(unittest.TestCase):

    def test_map_reduce_sch_1(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        pass

    def create_map_reduce_graph(num_map_nodes, num_shuffle_nodes, num_reduce_nodes):
        '''
            Function to create map-reduce graph
            num_map_nodes: number of map nodes
            num_shuffle_nodes: number of shuffle nodes
            num_reduce_nodes: number of reduce nodes
        '''
        pass