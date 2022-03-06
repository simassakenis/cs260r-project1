import unittest
import networkx as nx
from simulator.nodes import LogicalNode, PhysicalNode, MapNode, Input
from simulator.daskscheduler import DaskScheduler
from simulator.simulator import simulate
from simulator.nodes import Config

class TestDaskScheduler(unittest.TestCase):
    def setUp(self):
        Config.FAILURE_PROBABILITY = 0
        Config.STRAGGLER_PROBABILITY = 0
    
    def tearDown(self):
        Config.reset()

    def test_dask_scheduler_1(self):
        # 1 input node, 1 output node, 1 physical node
        num_physical_nodes = 1
        compute_power = 1
        memory = 1
        bandwidth = 1

        # This is very hacky
        # Maybe would pay off to just use networkx all the way?
        logical_graph = nx.DiGraph()

        # Add input nodes and input sizes
        logical_graph.add_nodes_from([
            (0, {"input": Input(size=1, timestamp=0), "logical_node": MapNode()})
        ])

        # Add the rest of the nodes
        logical_graph.add_nodes_from([
            (1, {"logical_node": LogicalNode()})
        ])

        # Add edges
        logical_graph.add_edges_from([
            (0, 1)
        ])

        # Create logical nodes
        logical_nodes = TestDaskScheduler.create_logical_nodes(logical_graph)
        
        # Create physical nodes
        physical_nodes = TestDaskScheduler.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # Simulate the computation
        total_time = simulate(logical_nodes, physical_nodes, DaskScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 3)

    def test_dask_scheduler_2(self):
        # 1 input node, 1 output node, 1 physical node
        num_physical_nodes = 1
        compute_power = 1
        memory = 1
        bandwidth = 1

        # This is very hacky
        # Maybe would pay off to just use networkx all the way?
        logical_graph = nx.DiGraph()

        # Add input nodes and input sizes
        logical_graph.add_nodes_from([
            (0, {"input": Input(size=1, timestamp=0), "logical_node": MapNode()})
        ])

        # Add the rest of the nodes
        logical_graph.add_nodes_from([
            (1, {"logical_node": LogicalNode()}),
            (2, {"logical_node": LogicalNode()})
        ])

        # Add edges
        logical_graph.add_edges_from([
            (0, 1),
            (1, 2)
        ])

        # Create logical nodes
        logical_nodes = TestDaskScheduler.create_logical_nodes(logical_graph)

        # Create physical nodes
        physical_nodes = TestDaskScheduler.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # Simulate the computation
        total_time = simulate(logical_nodes, physical_nodes, DaskScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 5)

    def test_dask_scheduler_3(self):
        # 1 input node, 1 output node, 1 physical node
        num_physical_nodes = 1
        compute_power = 1
        memory = 1
        bandwidth = 1

        # This is very hacky
        # Maybe would pay off to just use networkx all the way?
        logical_graph = nx.DiGraph()

        # Add input nodes and input sizes
        logical_graph.add_nodes_from([
            (0, {"input": Input(size=1, timestamp=0), "logical_node": MapNode()})
        ])

        # Add the rest of the nodes
        logical_graph.add_nodes_from([
            (1, {"logical_node": LogicalNode()}),
            (2, {"logical_node": LogicalNode()}),
            (3, {"logical_node": LogicalNode()}),
            (4, {"logical_node": LogicalNode()}),
            (5, {"logical_node": LogicalNode()})
        ])

        # Add edges
        logical_graph.add_edges_from([
            (0, 1),
            (1, 2),
            (2, 5),
            (0, 3),
            (3, 4),
            (4, 5)
        ])

        # Create logical nodes
        logical_nodes = TestDaskScheduler.create_logical_nodes(logical_graph)

        # Create physical nodes
        physical_nodes = TestDaskScheduler.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # Simulate the computation
        total_time = simulate(logical_nodes, physical_nodes, DaskScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 12)

    def test_dask_scheduler_4(self):
        # 1 input node, 1 output node, 1 physical node
        num_physical_nodes = 2
        compute_power = 1
        memory = 1
        bandwidth = 1

        # This is very hacky
        # Maybe would pay off to just use networkx all the way?
        logical_graph = nx.DiGraph()

        # Add input nodes and input sizes
        logical_graph.add_nodes_from([
            (0, {"input": Input(size=1, timestamp=0), "logical_node": MapNode()})
        ])

        # Add the rest of the nodes
        logical_graph.add_nodes_from([
            (1, {"logical_node": LogicalNode()}),
            (2, {"logical_node": LogicalNode()}),
            (3, {"logical_node": LogicalNode()}),
            (4, {"logical_node": LogicalNode()}),
            (5, {"logical_node": LogicalNode()})
        ])

        # Add edges
        logical_graph.add_edges_from([
            (0, 1),
            (1, 2),
            (2, 5),
            (0, 3),
            (3, 4),
            (4, 5)
        ])

        # Create logical nodes
        logical_nodes = TestDaskScheduler.create_logical_nodes(logical_graph)

        # Create physical nodes
        physical_nodes = TestDaskScheduler.create_physical_nodes(
            num_physical_nodes, 
            [compute_power]*num_physical_nodes, 
            [memory]*num_physical_nodes, 
            [bandwidth]*num_physical_nodes)

        # Simulate the computation
        total_time = simulate(logical_nodes, physical_nodes, DaskScheduler)

        print("Total time: ",total_time)
        self.assertEqual(total_time, 10)

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