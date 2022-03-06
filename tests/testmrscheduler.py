import unittest
from simulator.nodes import LogicalNode, PhysicalNode, Input, MapNode, ReduceNode, ShuffleNode
from simulator.mrscheduler import MRScheduler
from simulator.simulator import simulate
from simulator.nodes import Config
from tests.mrhelperfunctions import MRHelperFunctions

class TestMRScheduler(unittest.TestCase):

    def setUp(self):
        Config.FAILURE_PROBABILITY = 0
        Config.STRAGGLER_PROBABILITY = 0
    
    def tearDown(self):
        Config.reset()

    def test_map_reduce_sch_1(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        # simple map-reduce graph with 1 map node, 1 reduce node, 1 shuffle node, and 1 physical node
        num_map_nodes = 1
        num_reduce_nodes = 1
        map_computation_length = 1
        num_physical_nodes = 1
        compute_power = 1
        memory = 1
        bandwidth = 1

        # initializing Computation Graph
        map_nodes, shuffle_node, reduce_nodes = MRHelperFunctions.create_map_reduce_graph(num_map_nodes, [map_computation_length]*num_map_nodes, num_reduce_nodes)

        # creating physical nodes
        physical_nodes = MRHelperFunctions.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # logical nodes
        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.append(shuffle_node)
        logical_nodes.extend(reduce_nodes)
        
        # Simulating the computation
        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 7)

    
    def test_map_reduce_sch_2(self):
        '''
            Function to test simple map reduce with 2 map node, 2 reduce node, and 2 physical node
        '''
        
        num_map_nodes = 2
        num_reduce_nodes = 2
        map_computation_length = 1
        num_physical_nodes = 2
        compute_power = 1
        memory = 1
        bandwidth = 1

        # initializing Computation Graph
        map_nodes, shuffle_node, reduce_nodes = MRHelperFunctions.create_map_reduce_graph(num_map_nodes, [map_computation_length]*num_map_nodes, num_reduce_nodes)

        # creating physical nodes
        physical_nodes = MRHelperFunctions.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # logical nodes
        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.append(shuffle_node)
        logical_nodes.extend(reduce_nodes)
        
        # Simulating the computation
        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 9)

    def test_map_reduce_sch_3(self):
        '''
            Function to test simple map reduce with 2 map node, 2 reduce node, and 2 physical node
        '''
        
        num_map_nodes = 4
        num_reduce_nodes = 2
        map_computation_length = 1
        num_physical_nodes = 2
        compute_power = 1
        memory = 1
        bandwidth = 1

        # initializing Computation Graph
        map_nodes, shuffle_node, reduce_nodes = MRHelperFunctions.create_map_reduce_graph(num_map_nodes, [map_computation_length]*num_map_nodes, num_reduce_nodes)

        # creating physical nodes
        physical_nodes = MRHelperFunctions.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # logical nodes
        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.append(shuffle_node)
        logical_nodes.extend(reduce_nodes)
        
        # Simulating the computation
        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 15)

    def test_map_reduce_sch_4(self):
        '''
            Function to test simple map reduce with 2 map node, 2 reduce node, and 2 physical node
        '''
        
        num_map_nodes = 30
        num_reduce_nodes = 8
        map_computation_length = 1
        num_physical_nodes = 8
        compute_power = 1
        memory = 1
        bandwidth = 1

        # initializing Computation Graph
        map_nodes, shuffle_node, reduce_nodes = MRHelperFunctions.create_map_reduce_graph(num_map_nodes, [map_computation_length]*num_map_nodes, num_reduce_nodes)

        # creating physical nodes
        physical_nodes = MRHelperFunctions.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # logical nodes
        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.append(shuffle_node)
        logical_nodes.extend(reduce_nodes)
        
        # Simulating the computation
        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 65)
