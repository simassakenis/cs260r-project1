from simulator.nodes import LogicalNode, PhysicalNode, LogicalNodeState, LogicalNodeType

class MRScheduler:
    @staticmethod
    def schedule(logical_nodes: list[LogicalNode], physical_nodes: list[PhysicalNode]):
        '''
            Function to schedule the logical nodes to physical nodes
            logical_nodes: list of logical nodes to schedule
            physical_nodes: list of physical nodes to schedule to
        '''

        # Scheduling will be different here. Start with shuffle node, if it can be scheduled, then schedule map nodes, then reduce nodes.
        scheduled_pairs = []

        # List of all physical nodes not scheduled
        remaining_physical_nodes = list(filter(lambda x: x.schedulable(), physical_nodes))

        # Scheduling shuffle node
        shuffle_node = next(filter(lambda x: x.type == LogicalNodeType.SHUFFLE, logical_nodes), None)
        if shuffle_node is not None and shuffle_node.schedulable():
            best_physical_node = MRScheduler.find_best_physical_node(shuffle_node, remaining_physical_nodes)
            if best_physical_node is not None:
                scheduled_pairs.append((shuffle_node, best_physical_node))
                remaining_physical_nodes.remove(best_physical_node)
        
        # Scheduling map nodes
        map_nodes = list(filter(lambda x: x.type == LogicalNodeType.MAP, logical_nodes))
        for map_node in map_nodes:
            if map_node.schedulable():
                best_physical_node = MRScheduler.find_best_physical_node(map_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((map_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)
        
        # Scheduling reduce nodes
        reduce_nodes = list(filter(lambda x: x.type == LogicalNodeType.REDUCE, logical_nodes))
        for reduce_node in reduce_nodes:
            if reduce_node.schedulable():
                best_physical_node = MRScheduler.find_best_physical_node(reduce_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((reduce_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)

        # Scheduling other nodes
        other_nodes = list(filter(lambda x: x.type != LogicalNodeType.OTHER, logical_nodes))
        for other_node in other_nodes:
            if other_node.schedulable():
                best_physical_node = MRScheduler.find_best_physical_node(other_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((other_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)
        
        return scheduled_pairs

    @staticmethod
    def find_best_physical_node(logical_node: LogicalNode, physical_nodes: list[PhysicalNode]):
        '''
            Function to find the best physical node for the logical node
            logical_node: logical node to score
            physical_node: physical node to score
        '''
        # For now do a very simple score. Later check for spec compatibility, bandwidth, and localization
        best_physical_node = None
        best_score = 0

        # for each physical node find the best score
        for physical_node in remaining_physical_nodes:
            score = MRScheduler.__score_physical_node(logical_node, physical_node)
            if score > best_score:
                best_physical_node = physical_node
                best_score = score
        return best_physical_node

    @staticmethod
    def __score_physical_node(logical_node: LogicalNode, physical_node: PhysicalNode):
        '''
            Function to score the physical node for the logical node
            logical_node: logical node to score
            physical_node: physical node to score
        '''
        # For now do a very simple score. Later check for spec compatibility, bandwidth, and localization
        if(physical_node.schedulable()):
            return 1
        return -1