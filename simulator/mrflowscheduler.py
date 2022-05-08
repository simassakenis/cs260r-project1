from simulator.nodes import LogicalNode, PhysicalNode, LogicalNodeState, LogicalNodeType

class MRFlowScheduler:

    MAX_SHUFFLE_NODES = 100000
    MAX_REDUCE_NODES = 100000

    @staticmethod
    def schedule(logical_nodes: list[LogicalNode], physical_nodes: list[PhysicalNode], completed_nodes: list[LogicalNode] = [], failed_nodes: list[LogicalNode] = []):
        '''
            Function to schedule the logical nodes to physical nodes
            logical_nodes: list of logical nodes to schedule
            physical_nodes: list of physical nodes to schedule to
            completed_nodes: list of logical nodes that completed in the last loop iteration
            failed_nodes: list of logical nodes that failed in the last loop iteration
        '''

        # Scheduling will be different here. Start with shuffle node, if it can be scheduled, then schedule map nodes, then reduce nodes.
        scheduled_pairs = []

        # find all running nodes
        running_nodes = list(filter(lambda x: x.state is LogicalNodeState.COMPUTING or x.state is LogicalNodeState.NEED_INPUT, logical_nodes))
        m_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.MAP, running_nodes)))
        s_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.SHUFFLE, running_nodes)))
        r_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.REDUCE, running_nodes)))

        # List of all physical nodes not scheduled
        remaining_physical_nodes = list(filter(lambda x: x.schedulable(), physical_nodes))
        if len(remaining_physical_nodes) == 0:
            return scheduled_pairs

        # Scheduling shuffle node
        shuffle_nodes = filter(lambda x: x.type == LogicalNodeType.SHUFFLE and x.schedulable(), logical_nodes)
        for shuffle_node in shuffle_nodes:
            if shuffle_node.schedulable() and len(remaining_physical_nodes) > 0 and s_running_nodes < MRScheduler.MAX_SHUFFLE_NODES:
                best_physical_node = MRScheduler.find_best_physical_node(shuffle_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((shuffle_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)
                    s_running_nodes += 1

        if len(remaining_physical_nodes) == 0:
            return scheduled_pairs
        
        # Scheduling map nodes
        map_nodes = list(filter(lambda x: x.type == LogicalNodeType.MAP and x.schedulable(), logical_nodes))
        for map_node in map_nodes:
            if map_node.schedulable() and len(remaining_physical_nodes) > 0:
                best_physical_node = MRScheduler.find_best_physical_node(map_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((map_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)

        if len(remaining_physical_nodes) == 0:
            return scheduled_pairs
        
        # Scheduling reduce nodes
        reduce_nodes = list(filter(lambda x: x.type == LogicalNodeType.REDUCE and x.schedulable(), logical_nodes))
        for reduce_node in reduce_nodes:
            if reduce_node.schedulable() and len(remaining_physical_nodes) > 0 and m_running_nodes < MRScheduler.MAX_REDUCE_NODES:
                best_physical_node = MRScheduler.find_best_physical_node(reduce_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((reduce_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)
                    r_running_nodes += 1

        if len(remaining_physical_nodes) == 0:
            return scheduled_pairs

        # Scheduling other nodes
        other_nodes = list(filter(lambda x: x.type == LogicalNodeType.OTHER and x.schedulable(), logical_nodes))
        for other_node in other_nodes:
            if other_node.schedulable():
                best_physical_node = MRScheduler.find_best_physical_node(other_node, remaining_physical_nodes)
                if best_physical_node is not None:
                    scheduled_pairs.append((other_node, best_physical_node))
                    remaining_physical_nodes.remove(best_physical_node)

        return scheduled_pairs

    @staticmethod
    def find_best_physical_node(logical_node: LogicalNode, remaining_physical_nodes: list[PhysicalNode]):
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
                break
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
