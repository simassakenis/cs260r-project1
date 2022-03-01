# The simulator sits in a loop and iteratively updates the state of
# the system. This state consists of status indications for all
# logical nodes. A logical node can be in one of four states:
# (1) “not scheduled”, (2) “waiting for inputs”, (3) “computing”,
# (4) “completed”. Each logical node must go through these states
# in this order (potentially skipping some states). The execution
# starts with all logical nodes in the “not scheduled” state and
# ends when all logical nodes are in the “completed” state. At each
# timestep, the simulator observes the current state of the
# system and determines the next state.

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


def scheduler(logical_nodes, physical_nodes):
    #TODO
    pass


def failure(logical_nodes):
    #TODO
    pass


def simulator(logical_nodes, physical_nodes):
    '''
        
    '''
    current_timestamp = 0
    while True:
        new_logical_to_physical_assignments = scheduler(logical_nodes, physical_node)
        for logical_node, physical_node in new_logical_to_physical_assignments:
            assert logical_node.state is LogicalNodeState.NOT_SCHEDULED
            logical_node.phys_node = physical_node
            # TODO: update input_q timestamps
            for inp in logical_node.input_q:
                inp.timestamp = current_timestamp + int(inp.data_size / physical_node.bandwidth)
            logical_node.state = LogicalNodeState.WAITING_FOR_INPUTS
            physical_node.current_logical_node = logical_node

        for logical_node in logical_nodes:
            if logical_node.state is LogicalNodeState.WAITING_FOR_INPUTS:
                if (len(logical_node.input_q) == logical_node.number_of_inputs
                    and max([inp.timestamp for inp in logical_node.input_q]) <= current_timestamp):
                    logical_node.state = LogicalNodeState.COMPUTING
                    input_sz = sum([inp.data_size for inp in logical_node.input_q])
                    logical_node.computation_timestamp = (current_timestamp
                        + logical_node.computation_length)

            if logical_node.state is LogicalNodeState.COMPUTING:
                if logical_node.computation_timestamp == current_timestamp:
                    logical_node.state = LogicalNodeState.COMPLETED
                    #TODO: add outputs to the input queues of out-neighbors
                    for out_node in logical_node.out_neighbors:
                        inp = Input(data_size=logical_node.output_size, timestamp=None, source_node=logical_node)
                        out_node.input_q.append(inp)

        if all([logical_node.state is LogicalNodeState.COMPLETED
                for logical_node in logical_nodes]):
            return current_timestamp

        failed_nodes = failure(logical_nodes)
        for logical_node in failed_nodes:
            assert logical_node.state in ['waiting for inputs', 'computing']
            #TODO: (potentially) reset some fields of logical_node
            logical_node.state = LogicalNodeState.FAILED

        current_timestamp += 1


# if __name__ == '__main__':
#     logical_nodes = #TODO (create logical graph)
#     execution_time = simulator(logical_nodes)
