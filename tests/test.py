import unittest
from simulator.nodes import LogicalNode, PhysicalNode, Input, MapNode, ReduceNode
from simulator.simulator import simulate
import random
from tests.helperfunctions import HelperFunctions
from simulator.simplequeuescheduler import SimpleQueueScheduler

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

        map_node = MapNode(ninputs = 1, input_q = [Input(1,0, None)])

        reduce_node = ReduceNode(ninputs = 1)

        # connecting map node to reduce node
        map_node.out_neighbors.append(reduce_node)
        reduce_node.in_neighbors.append(map_node)

        logical_nodes = [map_node, reduce_node]

        # creating physical node
        physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)

        total_time = simulate(logical_nodes, [physical_node], SimpleQueueScheduler)
        print("Total time: ",total_time)
        self.assertEqual(total_time, 4)
    
    def test_map_reduce_2(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        # initializing Computation Graph
        num_map_nodes = 1
        num_reduce_nodes = 1
        map_computation_length = 1
        reduce_computation_length = 1

        map_node = MapNode(ninputs = 1, input_q = [Input(2,0, None)])

        reduce_node = ReduceNode(ninputs = 1)

        # connecting map node to reduce node
        map_node.out_neighbors.append(reduce_node)
        reduce_node.in_neighbors.append(map_node)

        logical_nodes = [map_node, reduce_node]

        # creating physical node
        physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)

        total_time = simulate(logical_nodes, [physical_node], SimpleQueueScheduler)
        print("Total time: ",total_time)
        self.assertEqual(total_time, 7)

    def test_map_reduce_3(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        # initializing Computation Graph
        num_map_nodes = 1
        num_reduce_nodes = 1
        map_computation_length = 1
        reduce_computation_length = 1

        map_node = MapNode(ninputs = 1, input_q = [Input(3,0, None)])

        reduce_node = ReduceNode(ninputs = 1)

        # connecting map node to reduce node
        map_node.out_neighbors.append(reduce_node)
        reduce_node.in_neighbors.append(map_node)

        logical_nodes = [map_node, reduce_node]

        # creating physical node
        physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)

        total_time = simulate(logical_nodes, [physical_node], SimpleQueueScheduler)
        print("Total time: ",total_time)
        self.assertEqual(total_time, 10)

    def test_map_reduce_4(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        # initializing Computation Graph
        num_map_nodes = 2
        num_reduce_nodes = 2
        map_computation_length = 1
        reduce_computation_length = 1

        map_nodes = HelperFunctions.create_map_nodes(num_map_nodes, (1,2), (1, 2))

        reduce_nodes = HelperFunctions.create_reduce_nodes(num_reduce_nodes, (1,2), (1, 2), map_nodes)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(reduce_nodes)

        # connecting map node to reduce node
        HelperFunctions.connect_reduce_nodes_to_map_nodes(map_nodes, reduce_nodes, 1)

        # creating physical node
        physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)

        total_time = simulate(logical_nodes, [physical_node], SimpleQueueScheduler)
        print("Total time: ",total_time)
        self.assertEqual(total_time, 11)




if __name__ == '__main__':
    unittest.main()