from simulator.nodes import LogicalNode, PhysicalNode, LogicalNodeState

class SimpleQueueScheduler:
    @staticmethod
    def schedule(logical_nodes: list[LogicalNode], physical_nodes: list[PhysicalNode]):
        '''
            Function to schedule the logical nodes to physical nodes
            logical_nodes: list of logical nodes to schedule
            physical_nodes: list of physical nodes to schedule to
        '''

        # List of all logical nodes not scheduled and failed with all inputs present
        remaining_logical_nodes = list(filter(lambda x: x.schedulable() and x.inputs_present(), logical_nodes))
        
        # List of all physical nodes not scheduled
        remaining_physical_nodes = list(filter(lambda x: x.schedulable(), physical_nodes))

        # list of scheduled pairs (logical_node, physical_node)
        scheduled_pairs = []


        # for each logical node find the best physical node
        for logical_node in remaining_logical_nodes:
            best_physical_node = None
            best_score = 0

            # for each physical node find the best score
            for physical_node in remaining_physical_nodes:
                score = SimpleQueueScheduler.__score_physical_node(logical_node, physical_node)
                if score > best_score:
                    best_physical_node = physical_node
                    best_score = score

            # assign the best physical node to the logical node
            if best_physical_node is not None:
                scheduled_pairs.append((logical_node, best_physical_node))
                remaining_physical_nodes.remove(best_physical_node)
        
        return scheduled_pairs

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