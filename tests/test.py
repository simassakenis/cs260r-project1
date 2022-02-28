import unittest
from simulator.simulator import LogicalNode, PhysicalNode
import random
from helperfunctions import HelperFunctions

class MapReduceTest(unittest.TestCase):

    def test_simple_map_reduce(self):
        '''
            Function to test simple map reduce with 3 map nodes and 2 reduce nodes
        '''
        # initializing Computation Graph
        num_map_nodes = 3
        num_reduce_nodes = 2
        map_computation_length = 3
        reduce_computation_length = 3

        # initializing Map Nodes
        map_nodes = HelperFunctions.create_map_nodes(num_map_nodes, (map_computation_length, map_computation_length+1), (1,2))
        
        # initializing Reduce Nodes
        reduce_nodes = HelperFunctions.create_reduce_nodes(num_reduce_nodes, (reduce_computation_length, reduce_computation_length+1), (1,2), map_nodes)

        # connecting Reduce Nodes to Map Nodes
        map_nodes, reduce_nodes = HelperFunctions.connect_reduce_nodes_to_map_nodes(map_nodes, reduce_nodes, 1)

        # add physical nodes
        physical_nodes = HelperFunctions.create_physical_nodes(3, (1,2), (1,2))



if __name__ == '__main__':
    unittest.main()