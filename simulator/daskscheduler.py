from simulator.nodes import LogicalNode, PhysicalNode, LogicalNodeState, LogicalNodeType

class DaskScheduler:
    @staticmethod
    def schedule(logical_nodes: list[LogicalNode],
                 physical_nodes: list[PhysicalNode],
                 completed_lnodes: list[LogicalNode],
                 failed_lnodes: list[LogicalNode]):
        scheduled_pairs = []

        remaining_physical_nodes = list(filter(lambda x: x.schedulable(), physical_nodes))
        ready_logical_nodes = list(filter(lambda x: x.schedulable(), logical_nodes))

        # Schedule first to satisfy dependencies
        # If there are any out neighbors who can run, schedule them on the same
        # physical node
        # This is equivalent to LIFO
        for logical_node in completed_lnodes:
            picked_neighbor = None
            for neighbor in logical_node.out_neighbors:
                if neighbor.schedulable() and neighbor in ready_logical_nodes:
                    picked_neighbor = neighbor
                    break
            if picked_neighbor is not None:
                scheduled_pairs.append((picked_neighbor, logical_node.pnode))
                remaining_physical_nodes.remove(logical_node.pnode)
                ready_logical_nodes.remove(picked_neighbor)

        # Does it matter for the rest? Probably not
        for logical_node in ready_logical_nodes:
            best_physical_node = DaskScheduler.find_best_physical_node(logical_node, remaining_physical_nodes)
            if best_physical_node is not None:
                scheduled_pairs.append((logical_node, best_physical_node))
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
            score = DaskScheduler.__score_physical_node(logical_node, physical_node)
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