class TestMRScheduler(unittest.TestCase):

    def test_map_reduce_sch_1(self):
        '''
            Function to test simple map reduce with 1 map node, 1 reduce node, and 1 physical node
        '''
        # initializing Computation Graph
        num_map_nodes = 1
        num_reduce_nodes = 1
        map_computation_length = 1
        reduce_computation_length = 1

        map_node = LogicalNode(ninputs = 1, input_q = [Input(1,0, None)])

        reduce_node = LogicalNode(ninputs = 1)

        # connecting map node to reduce node
        map_node.out_neighbors.append(reduce_node)
        reduce_node.in_neighbors.append(map_node)

        logical_nodes = [map_node, reduce_node]

        # creating physical node
        physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)

        total_time = simulate(logical_nodes, [physical_node])
        print("Total time: ",total_time)
        self.assertEqual(total_time, 4)

    def create_map_reduce_graph(num_map_nodes, num_shuffle_nodes, num_reduce_nodes):
        '''
            Function to create map-reduce graph
            num_map_nodes: number of map nodes
            num_shuffle_nodes: number of shuffle nodes
            num_reduce_nodes: number of reduce nodes
        '''
        pass