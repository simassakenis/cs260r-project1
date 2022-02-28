import unittest
from simulator.simulator import LogicalNode, PhysicalNode, LogicalNodeState, PhysicalNodeState
from simulator.simplequeuescheduler import SimpleQueueScheduler
from tests.helperfunctions import HelperFunctions


class TestSimpleQueueScheduler(unittest.TestCase):

    def test_simple_queue_scheduler_1(self):
        '''
            Function to test simple queue scheduler with 3 map nodes and 2 reduce nodes
        '''
        # initializing Computation Graph
        num_map_nodes = 1
        num_reduce_nodes = 1
        num_physical_nodes = 1
        map_computation_length = 3
        reduce_computation_length = 3

        # initializing Map Nodes
        map_nodes = HelperFunctions.create_map_nodes(
            num_map_nodes, (map_computation_length, map_computation_length+1), (1, 2))

        # initializing Reduce Nodes
        reduce_nodes = HelperFunctions.create_reduce_nodes(
            num_reduce_nodes, (reduce_computation_length, reduce_computation_length+1), (1, 2), map_nodes)

        # connecting Reduce Nodes to Map Nodes
        map_nodes, reduce_nodes = HelperFunctions.connect_reduce_nodes_to_map_nodes(
            map_nodes, reduce_nodes, 1)

        # logical nodes
        logical_nodes = []
        logical_nodes.extend(map_nodes)
        logical_nodes.extend(reduce_nodes)

        # creating physical nodes
        physical_nodes = HelperFunctions.create_physical_nodes(
            num_physical_nodes, (1, 2), (1, 2), (1, 2))

        # test the scheduler
        scheduled_pairs = SimpleQueueScheduler.schedule(logical_nodes, physical_nodes)

        for sp in scheduled_pairs:
            print('logical node: ', sp[0].node_id, 'physical node: ', sp[1].node_id)
        
        self.assertEqual(len(scheduled_pairs), 1)
        self.assertEqual(scheduled_pairs[0][0].node_id, 0)
        self.assertEqual(scheduled_pairs[0][1].node_id, 0)

        # marking first logical node as completed
        logical_nodes[0].state = LogicalNodeState.COMPLETED

        # test the scheduler
        scheduled_pairs = SimpleQueueScheduler.schedule(logical_nodes, physical_nodes)

        for sp in scheduled_pairs:
            print('logical node: ', sp[0].node_id, 'physical node: ', sp[1].node_id)
        
        self.assertEqual(len(scheduled_pairs), 1)
        self.assertEqual(scheduled_pairs[0][0].node_id, 1)
        self.assertEqual(scheduled_pairs[0][1].node_id, 0)


if __name__ == '__main__':
    unittest.main()