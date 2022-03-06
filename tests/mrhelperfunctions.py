from simulator.nodes import *

class MRHelperFunctions:
    @staticmethod
    def create_map_reduce_graph(num_map_nodes, input_map_sizes, num_reduce_nodes):
        '''
            Function to create map-reduce graph
            num_map_nodes: number of map nodes
            num_reduce_nodes: number of reduce nodes
        '''
        # creating map nodes
        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, input_map_sizes)

        # creating shuffle node
        shuffle_node = ShuffleNode(output_length=MRHelperFunctions.shuffle_output_length)

        # connect map nodes to shuffle node
        for map_node in map_nodes:
            map_node.out_neighbors.append(shuffle_node)
            shuffle_node.in_neighbors.append(map_node)
        
        # creating reduce nodes
        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect map nodes to reduce nodes
        for reduce_node in reduce_nodes:
            for map_node in map_nodes:
                map_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(map_node)

        # connect shuffle nodes to reduce nodes
        for reduce_node in reduce_nodes:
            shuffle_node.out_neighbors.append(reduce_node)
            reduce_node.in_neighbors.append(shuffle_node)

        return map_nodes, shuffle_node, reduce_nodes
        

    @staticmethod
    def create_map_nodes(num_map_nodes, input_sizes):
        '''
            Function to create map nodes
            num_map_nodes: number of map nodes
        '''
        assert len(input_sizes) == num_map_nodes

        # create map nodes
        map_nodes = []
        for i in range(num_map_nodes):
            map_nodes.append(MapNode(input_q=[Input(input_sizes[i], None, None)]))

        return map_nodes

    @staticmethod
    def shuffle_output_length(size):
        return 0

    
    @staticmethod
    def create_reduce_nodes(num_reduce_nodes):
        '''
            Function to create reduce nodes
            num_reduce_nodes: number of reduce nodes
        '''
        reduce_nodes = []
        for i in range(num_reduce_nodes):
            reduce_nodes.append(ReduceNode())

        return reduce_nodes
    
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