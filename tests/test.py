import unittest
from simulator.nodes import LogicalNode, PhysicalNode, Input
from simulator.simulator import simulate
import random
from tests.helperfunctions import HelperFunctions

class MapReduceTest(unittest.TestCase):

    def test_map_reduce_1(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        # initializing Computation Graph
        num_map_nodes = 1
        num_reduce_nodes = 1
        map_computation_length = 1
        reduce_computation_length = 1

        map_node = LogicalNode(number_of_inputs = 1, computation_length = map_computation_length, output_size = 1, input_q = [Input(1,0, None)])

        reduce_node = LogicalNode(number_of_inputs = 1, computation_length = reduce_computation_length, output_size = 1)

        # connecting map node to reduce node
        map_node.out_neighbors.append(reduce_node)
        reduce_node.in_neighbors.append(map_node)

        logical_nodes = [map_node, reduce_node]

        # creating physical node
        physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)

        total_time = simulate(logical_nodes, [physical_node])
        print("Total time: ",total_time)
        self.assertEqual(total_time, 5)




if __name__ == '__main__':
    unittest.main()