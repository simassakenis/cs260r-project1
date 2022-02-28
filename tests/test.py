import unittest
from simulator.simulator import LogicalNode, PhysicalNode
import random

class MapReduceTest(unittest.TestCase):

    def test_simple_map_reduce(self):
        '''
            Function to test simple map reduce with 3 map nodes and 2 reduce nodes
        '''
        # initializing Computation Graph
        numMapNodes = 3
        numReduceNodes = 2
        mapComputationLength = 3
        reduceComputationLength = 3

        # initializing Map Nodes
        map_nodes = HelperFunctions.create_map_nodes(numMapNodes, (mapComputationLength, mapComputationLength+1), (1,2))
        
        # initializing Reduce Nodes
        reduce_nodes = HelperFunctions.create_reduce_nodes(numReduceNodes, (reduceComputationLength, reduceComputationLength+1), (1,2), map_nodes)

        # connecting Reduce Nodes to Map Nodes
        map_nodes, reduce_nodes = HelperFunctions.connect_reduce_nodes_to_map_nodes(map_nodes, reduce_nodes, 1)

        # add physical nodes
        physical_nodes = HelperFunctions.create_physical_nodes(3, (1,2), (1,2))

    


class HelperFunctions:

    @staticmethod
    def create_physical_node(compute_power_range, memory_range, bandwidth_range):
        '''
            Function to create physical node
            compute_power_range: range of compute power for physical nodes
            memory_range: range of memory for physical nodes
            bandwidth_range: range of bandwidth for physical nodes
        '''
        physical_node = PhysicalNode(
            compute_power=random.randrange(compute_power_range[0], compute_power_range[1]),
            memory=random.randrange(memory_range[0], memory_range[1]),
            bandwidth=random.randrange(bandwidth_range[0], bandwidth_range[1]))
        
        return physical_node
    
    @staticmethod
    def create_physical_nodes(num_physical_nodes, compute_power_range, memory_range, bandwidth_range):
        '''
            Function to create physical node
            num_physical_nodes: number of physical nodes to create
            compute_power_range: range of compute power for physical nodes
            memory_range: range of memory for physical nodes
            bandwidth_range: range of bandwidth for physical nodes
        '''
        physical_nodes = []
        for i in range(num_physical_nodes):
            physical_node = HelperFunctions.create_physical_node(compute_power_range, memory_range, bandwidth_range)
            physical_nodes.append(physical_node)
        return physical_nodes

    @staticmethod
    def create_map_node(map_computation_range, map_output_range):
        '''
            Function to create map nodes randomly
            map_computation_range: range of computation length for map nodes
            map_output_range: range of output size for map nodes
        '''
        logical_map_node = LogicalNode(
            number_of_inputs=0, 
            computation_length=random.randrange(map_computation_range[0],map_computation_range[1]), 
            output_size=random.randrange(map_output_range[0],map_output_range[1]))
        return logical_map_node

    @staticmethod
    def create_map_nodes(num_map_nodes, map_computation_range, map_output_range):
        '''
            Function to create map nodes randomly
            num_map_nodes: number of map nodes to create
            map_computation_range: range of computation length for map nodes
            map_output_range: range of output size for map nodes
        '''
        map_nodes = []
        for i in range(num_map_nodes):
            logical_map_node = HelperFunctions.create_map_node(map_computation_range, map_output_range)
            map_nodes.append(logical_map_node)
        return map_nodes
    
    @staticmethod
    def create_reduce_node(reduce_computation_range, reduce_output_range, map_nodes):
        '''
            Function to create reduce nodes randomly
            reduce_computation_range: range of computation length for reduce nodes
            reduce_output_range: range of output size for reduce nodes
            map_nodes: list of map nodes
        '''
        logical_reduce_node = LogicalNode(
            computation_length=random.randrange(reduce_computation_range[0],reduce_computation_range[1]), 
            output_size=random.randrange(reduce_output_range[0],reduce_output_range[1]))
        return logical_reduce_node
    
    @staticmethod
    def create_reduce_nodes(num_reduce_nodes, reduce_computation_range, reduce_output_range, map_nodes):
        '''
            Function to create reduce nodes randomly
            num_reduce_nodes: number of reduce nodes to create
            reduce_computation_range: range of computation length for reduce nodes
            reduce_output_range: range of output size for reduce nodes
            map_nodes: list of map nodes
        '''
        reduce_nodes = []
        for i in range(num_reduce_nodes):
            logical_reduce_node = HelperFunctions.create_reduce_node(reduce_computation_range, reduce_output_range, map_nodes)
            reduce_nodes.append(logical_reduce_node)
        return reduce_nodes

    @staticmethod
    def connect_reduce_nodes_to_map_nodes(map_nodes, reduce_nodes, connection_ratio=0.5):
        '''
            Function to connect reduce nodes to map nodes
            map_nodes: list of map nodes
            reduce_nodes: list of reduce nodes
            connection_ratio: ratio of connections to make between map and reduce nodes
        '''
        # connecting Reduce Nodes to Map Nodes
        for rNode in reduce_nodes:
            # randomly connect to map nodes
            selected_map_nodes = random.sample(map_nodes, int(connection_ratio * len(map_nodes)))
            rNode.in_neighbors = selected_map_nodes
            rNode.number_of_inputs = len(selected_map_nodes)
            for mNode in selected_map_nodes:
                mNode.out_neighbors.append(rNode)
                # should we modify output size of map node?
        return map_nodes, reduce_nodes

if __name__ == '__main__':
    unittest.main()