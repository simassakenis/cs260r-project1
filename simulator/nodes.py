from enum import Enum

logical_node_count = 0
physical_node_count = 0

# Logical Node state enum
class LogicalNodeState(Enum):
    NOT_SCHEDULED = 1
    WAITING_FOR_INPUTS = 2
    COMPUTING = 3
    COMPLETED = 4
    FAILED = 5

# Logical Node state enum
class PhysicalNodeState(Enum):
    NOT_SCHEDULED = 1
    WAITING_FOR_INPUTS = 2
    COMPUTING = 3
    COMPLETED = 4
    FAILED = 5

class LogicalNode:
    def __init__(self, number_of_inputs=None, computation_length=None,
                output_size=None, computation_timestamp=None, phys_node=None,
                input_q=None, in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        global logical_node_count
        self.node_id = logical_node_count
        logical_node_count += 1
        self.number_of_inputs = number_of_inputs
        self.computation_length = computation_length
        self.output_size = output_size
        self.computation_timestamp = computation_timestamp
        self.phys_node = phys_node
        self.input_q = input_q if input_q is not None else []
        self.in_neighbors = in_neighbors if in_neighbors is not None else []
        self.out_neighbors = out_neighbors if out_neighbors is not None else []
        self.state = state
    def can_be_scheduled(self):
        return self.state is LogicalNodeState.NOT_SCHEDULED or self.state is LogicalNodeState.FAILED
    
    def is_all_input_present(self):
        return len(self.input_q) == self.number_of_inputs and None not in self.input_q

class PhysicalNode:
    def __init__(self, compute_power=None, memory=None,
                bandwidth=None, current_logical_node=None, state=PhysicalNodeState.NOT_SCHEDULED):
        global physical_node_count
        self.node_id = physical_node_count
        physical_node_count += 1
        self.compute_power = compute_power
        self.memory = memory
        self.bandwidth = bandwidth
        self.current_logical_node = current_logical_node
        self.state = state
    
    def can_be_scheduled(self):
        return self.state is PhysicalNodeState.NOT_SCHEDULED



class Input:
    def __init__(self, data_size=None, timestamp=None, source_node=None):
        self.data_size = data_size
        self.timestamp = timestamp
        self.source_node = source_node