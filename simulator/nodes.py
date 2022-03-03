from simulator.timer import Timer
from enum import Enum
import random

# Constants that should later be configurable

BANDWIDTH_MULTIPLIER = 1
STRAGGLER_SIZE_MULTIPLIER = 1
COMP_LENGTH_MULTIPLIER = 1
OUTPUT_SIZE_MULTIPLIER = 1

# Return the bandwidth multiplier from physical node1 to node2
# For now, assume uniform bandwidth
def bandwidth(node1, node2):
    global BANDWIDTH_MULTIPLIER
    return BANDWIDTH_MULTIPLIER

lnode_count = 0
pnode_count = 0

# Logical Node state enum
class LogicalNodeState(Enum):
    NOT_SCHEDULED = 1
    NEED_INPUT = 2
    COMPUTING = 3
    COMPLETED = 4
    FAILED = 5

class LogicalNodeType(Enum):
    MAP = 1
    REDUCE = 2
    SHUFFLE = 3
    OTHER = 4
    
# Extra time to add to a logical node as a straggler (for now just a small
# random chance for every node, but we could make it specific to certain sizes,
# etc)
def straggler_time(size):
    global STRAGGLER_SIZE_MULTIPLIER
    if random() < (1/1000):
        return STRAGGLER_SIZE_MULTIPLIER * size
    return 0

# Default functions for computation time and output size (just the size for now)

def default_comp_length(size):
    global COMP_LENGTH_MULTIPLIER
    return COMP_LENGTH_MULTIPLIER * size

def comp_length_with_straggler(size):
    global COMP_LENGTH_MULTIPLIER
    return COMP_LENGTH_MULTIPLIER * size + straggler_time(size)

def default_output_size(size):
    global OUTPUT_SIZE_MULTIPLIER
    return OUTPUT_SIZE_MULTIPLIER * size

class LogicalNode:
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_size=default_output_size,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED,
                type=LogicalNodeType.OTHER):
        global lnode_count
        self.id = lnode_count
        lnode_count += 1
        self.ninputs = ninputs
        self.comp_length = comp_length
        self.output_size = output_size
        self.input_size = None
        self.comp_finish_time = None
        self.pnode = pnode
        self.input_q = input_q if input_q is not None else []
        self.in_neighbors = in_neighbors if in_neighbors is not None else []
        self.out_neighbors = out_neighbors if out_neighbors is not None else []
        self.state = state
        self.type = type

    # Can this logical node be scheduled?
    def schedulable(self):
        return (self.state is LogicalNodeState.NOT_SCHEDULED or self.state is LogicalNodeState.FAILED) and self.inputs_present()

    # Are all the inputs present for this logical node (not necessarily arrived)
    def inputs_present(self):
        return len(self.input_q) == self.ninputs

class MapNode(LogicalNode):
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_size=default_output_size,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        super().__init__(ninputs, pnode, input_q, comp_length, output_size, in_neighbors, out_neighbors, state, LogicalNodeType.MAP)

class ReduceNode(LogicalNode):
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_size=default_output_size,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        super().__init__(ninputs, pnode, input_q, comp_length, output_size, in_neighbors, out_neighbors, state, LogicalNodeType.REDUCE)

class ShuffleNode(LogicalNode):
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_size=default_output_size,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        super().__init__(ninputs, pnode, input_q, comp_length, output_size, in_neighbors, out_neighbors, state, LogicalNodeType.SHUFFLE)

        def schedulable(self):
            return (self.state is LogicalNodeState.NOT_SCHEDULED or self.state is LogicalNodeState.FAILED) and len(self.input_q) > 0



class PhysicalNode:
    def __init__(self, compute_power=None, memory=None,
                bandwidth=None, lnode=None, failed=False):
        global pnode_count
        self.id = pnode_count
        pnode_count += 1
        self.compute_power = compute_power
        self.memory = memory
        self.bandwidth = bandwidth
        self.lnode = lnode
        self.failed = failed

    # Can this physical node be scheduled?
    def schedulable(self):
        return self.lnode is None and not self.failed

class Input:
    def __init__(self, size=None, timestamp=None, source=None):
        self.size = size
        self.timestamp = timestamp
        self.source = source

    # Update the timestamp of this input to arrive at the physical node `pnode`
    def update_time(self, timer, pnode):
        self.timestamp = timer.time_delta(self.size
                                            * bandwidth(self.source, pnode))
