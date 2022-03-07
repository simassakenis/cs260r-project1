import unittest
import networkx as nx
from mrhelperfunctions import MRHelperFunctions
from testbenchmark import Benchmarks
from simulator.nodes import LogicalNode, PhysicalNode, MapNode, Input
from simulator.daskscheduler import DaskScheduler
from simulator.simulator import simulate
from simulator.nodes import Config
from testdaskscheduler import TestDaskScheduler

class TestDaskBenchmark(unittest.TestCase):
    def setUp(self):
        Config.FAILURE_PROBABILITY = 0
        Config.STRAGGLER_PROBABILITY = 0
    
    def tearDown(self):
        Config.reset()

    def test_dask_sort(self):
        num_physical_nodes = 1800*4
        compute_power = 1/2
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0
        Config.STRAGGLER_LENGTH_MULTIPLIER = 2
        Config.FAILURE_PROBABILITY = 0
        Config.BANDWIDTH_MULTIPLIER = 1/1000

        physical_nodes = MRHelperFunctions.create_physical_nodes(num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        num_map_nodes = 15000
        map_input_size = 64
        map_assign_time = 1

        def map_output_size(input_size):
            return map_input_size
        
        def map_compute_length(input_size):
            return (map_input_size*compute_power)*2 + map_assign_time

        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, [map_input_size]*num_map_nodes)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_shuffle_nodes = 1800

        def shuffle_comp_length(input_size):
            return (input_size*compute_power)

        def shuffle_out_length(input_size):
            return input_size
        
        shuffle_nodes = MRHelperFunctions.create_shuffle_nodes(num_shuffle_nodes, shuffle_out_length)
        split_map_nodes = Benchmarks.split(map_nodes, num_shuffle_nodes)
        for shuffle_node, split in zip(shuffle_nodes, split_map_nodes):
            shuffle_node.comp_length = shuffle_comp_length
            # connect the map nodes to the shuffle nodes
            for map_node in split:
                map_node.out_neighbors.append(shuffle_node)
                shuffle_node.in_neighbors.append(map_node)

        num_reduce_nodes = num_shuffle_nodes

        def reduce_comp_length(input_size):
            return (input_size*compute_power)

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect reduce nodes to shuffle nodes
        for reduce_node, shuffle_node in zip(reduce_nodes, shuffle_nodes):
            reduce_node.comp_length = reduce_comp_length
            shuffle_node.out_neighbors.append(reduce_node)
            reduce_node.in_neighbors.append(shuffle_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(shuffle_nodes)
        logical_nodes.extend(reduce_nodes)

        total_time = simulate(logical_nodes, physical_nodes, DaskScheduler)

        print("Total time: ",total_time)

    @staticmethod
    def create_logical_nodes(logical_graph: nx.DiGraph):
        logical_nodes = []
        for node in logical_graph.nodes.data():
            logical_node = node[1]["logical_node"]

            # If in-degree is 0, it's an input node
            if logical_graph.in_degree(node[0]) == 0:
                logical_node.input_q = [node[1]["input"]]

            # Add out neighbors
            for out_neighbor in logical_graph.successors(node[0]):
                logical_node.out_neighbors.append(logical_graph.nodes[out_neighbor]["logical_node"])

            # Add in neighbors
            for in_neighbor in logical_graph.predecessors(node[0]):
                logical_node.in_neighbors.append(logical_graph.nodes[in_neighbor]["logical_node"])

            logical_nodes.append(logical_node)

        return logical_nodes

    @staticmethod
    def create_physical_nodes(num_physical_nodes, compute_power_array, memory_array, bandwidth_array):
        '''
            Function to create physical node
            num_physical_nodes: number of physical nodes to create
            compute_power_array: array of compute power of each physical node
            memory_array: array of memory of each physical node
            bandwidth_array: array of bandwidth of each physical node
        '''
        assert len(compute_power_array) == num_physical_nodes
        assert len(memory_array) == num_physical_nodes
        assert len(bandwidth_array) == num_physical_nodes

        physical_nodes = []
        for i in range(num_physical_nodes):
            physical_node = PhysicalNode(compute_power=compute_power_array[i], memory=memory_array[i], bandwidth=bandwidth_array[i])
            physical_nodes.append(physical_node)
        return physical_nodes

        