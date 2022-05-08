import unittest
from tests.finalhelpfunctions import MRHelperFunctions
from simulator.nodes import Config, straggler_time, Cluster
from simulator.simulator import simulate
from simulator.mrscheduler import MRScheduler
from collections import defaultdict
import random

class TestFinalBenchmarks(unittest.TestCase):
    '''
        Benchmarks from https://read.seas.harvard.edu/cs260r/2022/project1a/
    '''

    @staticmethod
    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

    def get_noise(self):
        return nprandom.normal(0, 0.1, 1)[0]


    def test_map_reduce_sort(self):
        num_physical_nodes = 1800*4
        compute_power = 1/2
        memory = 4000
        bandwidth = 1000

        Config.STRAGGLER_PROBABILITY = 0
        Config.STRAGGLER_LENGTH_MULTIPLIER = 2
        Config.FAILURE_PROBABILITY = 0
        Config.BANDWIDTH_MULTIPLIER = 1000

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

        # Add pnoed for map_nodes input
        map_split = TestFinalBenchmarks.split(map_nodes, num_physical_nodes)
        for sp, pn in zip(map_split, physical_nodes):
            for m in sp:
                m.input_q[0].set_from_pnode(pn)
        
        for mnode in map_nodes:
            mnode.output_length = map_output_size
            mnode.comp_length = map_compute_length
        
        num_shuffle_nodes = 1800

        def shuffle_comp_length(input_size):
            return (input_size*compute_power)

        def shuffle_out_length(input_size):
            return input_size
        
        shuffle_nodes = MRHelperFunctions.create_shuffle_nodes(num_shuffle_nodes, shuffle_out_length)
        split_map_nodes = TestFinalBenchmarks.split(map_nodes, num_shuffle_nodes)
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

        bandwidth = defaultdict(lambda: random.uniform(1000, 1500))
        latency = defaultdict(lambda: random.uniform(0.1, 0.2))

        cluster = Cluster(physical_nodes, bandwidth, latency)

        total_time = simulate(logical_nodes, physical_nodes, MRScheduler, cluster, True)

        print("Total time: ",total_time)