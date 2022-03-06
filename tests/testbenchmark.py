import unittest
from tests.mrhelperfunctions import MRHelperFunctions
from simulator.nodes import Config, straggler_time
from simulator.simulator import simulate
from simulator.mrscheduler import MRScheduler

class Benchmarks(unittest.TestCase):
    '''
        Benchmarks from https://read.seas.harvard.edu/cs260r/2022/project1a/
    '''

    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

    def get_noise(self):
        return nprandom.normal(0, 0.1, 1)[0]

    def test_map_reduce_grep(self):
        num_physical_nodes = 1800
        compute_power = 1/16
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0
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
            return 1/1000
        
        def map_compute_length(input_size):
            return (map_input_size*compute_power) + straggler_time(0.5) + map_assign_time

        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, [map_input_size]*num_map_nodes)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_reduce_nodes = 1

        def reduce_comp_length(input_size):
            return 1

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect map nodes to reduce nodes
        for reduce_node in reduce_nodes:
            reduce_node.comp_length = reduce_comp_length
            for map_node in map_nodes:
                map_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(map_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(reduce_nodes)

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)

    
    def test_map_reduce_grep_with_failure(self):
        num_physical_nodes = 1800
        compute_power = 1/16
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0
        Config.FAILURE_PROBABILITY = 0.001
        Config.BANDWIDTH_MULTIPLIER = 1/1000

        physical_nodes = MRHelperFunctions.create_physical_nodes(num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        num_map_nodes = 15000
        map_input_size = 64
        map_assign_time = 1

        def map_output_size(input_size):
            return 1/1000
        
        def map_compute_length(input_size):
            return (map_input_size*compute_power) + straggler_time(0.5) + map_assign_time

        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, [map_input_size]*num_map_nodes)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_reduce_nodes = 1

        def reduce_comp_length(input_size):
            return 1

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect map nodes to reduce nodes
        for reduce_node in reduce_nodes:
            reduce_node.comp_length = reduce_comp_length
            for map_node in map_nodes:
                map_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(map_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(reduce_nodes)

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)

    
    def test_map_reduce_grep_with_failure_straggler(self):
        num_physical_nodes = 1800
        compute_power = 1/16
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0.001
        Config.STRAGGLER_LENGTH_MULTIPLIER = 2
        Config.FAILURE_PROBABILITY = 0.001
        Config.BANDWIDTH_MULTIPLIER = 1/1000

        physical_nodes = MRHelperFunctions.create_physical_nodes(num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        num_map_nodes = 15000
        map_input_size = 64
        map_assign_time = 1

        def map_output_size(input_size):
            return 1/1000
        
        def map_compute_length(input_size):
            return (map_input_size*compute_power) + straggler_time(1) + map_assign_time

        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, [map_input_size]*num_map_nodes)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_reduce_nodes = 1

        def reduce_comp_length(input_size):
            return 1

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect map nodes to reduce nodes
        for reduce_node in reduce_nodes:
            reduce_node.comp_length = reduce_comp_length
            for map_node in map_nodes:
                map_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(map_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(reduce_nodes)

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler)

        print("Total time: ",total_time)


    def test_map_reduce_sort(self):
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
            return ((input_size/num_shuffle_nodes)*compute_power)

        def reduce_out_length(input_size):
            return input_size/num_shuffle_nodes

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect reduce nodes all shuffle nodes
        for reduce_node in reduce_nodes:
            reduce_node.comp_length = reduce_comp_length
            reduce_node.output_length = reduce_out_length
            for shuffle_node in shuffle_nodes:
                shuffle_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(shuffle_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(shuffle_nodes)
        logical_nodes.extend(reduce_nodes)

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler, False)

        print("Total time: ",total_time)

    def test_map_reduce_sort_with_straggler(self):
        num_physical_nodes = 1800*4
        compute_power = 1/2
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0.001
        Config.STRAGGLER_LENGTH_MULTIPLIER = 1
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
            return (map_input_size*compute_power)*2 + map_assign_time + straggler_time(input_size)

        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, [map_input_size]*num_map_nodes)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_shuffle_nodes = 1800

        def shuffle_comp_length(input_size):
            return (input_size*compute_power) + (straggler_time(input_size)/5)

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
            return ((input_size/num_shuffle_nodes)*compute_power) + (straggler_time(input_size/num_shuffle_nodes)/5)

        def reduce_out_length(input_size):
            return input_size/num_shuffle_nodes

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect reduce nodes all shuffle nodes
        for reduce_node in reduce_nodes:
            reduce_node.comp_length = reduce_comp_length
            reduce_node.output_length = reduce_out_length
            for shuffle_node in shuffle_nodes:
                shuffle_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(shuffle_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(shuffle_nodes)
        logical_nodes.extend(reduce_nodes)

        MRScheduler.MAX_SHUFFLE_NODES = num_shuffle_nodes
        MRScheduler.MAX_REDUCE_NODES = num_reduce_nodes

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler, False)

        print("Total time: ",total_time)

    def test_map_reduce_sort_with_straggler_Failure(self):
        num_physical_nodes = 1800*4
        compute_power = 1/2
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0.001
        Config.STRAGGLER_LENGTH_MULTIPLIER = 1
        Config.FAILURE_PROBABILITY = 0.001
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
            return (map_input_size*compute_power)*2 + map_assign_time + straggler_time(input_size)

        map_nodes = MRHelperFunctions.create_map_nodes(num_map_nodes, [map_input_size]*num_map_nodes)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_shuffle_nodes = 1800

        def shuffle_comp_length(input_size):
            return (input_size*compute_power) + (straggler_time(input_size)/5)

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
            return ((input_size/num_shuffle_nodes)*compute_power) + (straggler_time(input_size/num_shuffle_nodes)/5)

        def reduce_out_length(input_size):
            return input_size/num_shuffle_nodes

        reduce_nodes = MRHelperFunctions.create_reduce_nodes(num_reduce_nodes)

        # connect reduce nodes all shuffle nodes
        for reduce_node in reduce_nodes:
            reduce_node.comp_length = reduce_comp_length
            reduce_node.output_length = reduce_out_length
            for shuffle_node in shuffle_nodes:
                shuffle_node.out_neighbors.append(reduce_node)
                reduce_node.in_neighbors.append(shuffle_node)

        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(shuffle_nodes)
        logical_nodes.extend(reduce_nodes)

        MRScheduler.MAX_SHUFFLE_NODES = num_shuffle_nodes
        MRScheduler.MAX_REDUCE_NODES = num_reduce_nodes

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler, False)

        print("Total time: ",total_time)