from simulator.timer import Timer
from enum import Enum
import random

# Constants that should later be configurable

class Config:
    BANDWIDTH_MULTIPLIER = 1
    STRAGGLER_LENGTH_MULTIPLIER = 1
    COMP_LENGTH_MULTIPLIER = 1
    OUTPUT_LENGTH_MULTIPLIER = 1
    FAILURE_PROBABILITY = 0.001
    STRAGGLER_PROBABILITY = 0.001

    @staticmethod
    def reset():
        '''
            Function to reset the config
        '''
        Config.BANDWIDTH_MULTIPLIER = 1
        Config.STRAGGLER_LENGTH_MULTIPLIER = 1
        Config.COMP_LENGTH_MULTIPLIER = 1
        Config.OUTPUT_LENGTH_MULTIPLIER = 1
        Config.FAILURE_PROBABILITY = 0.001
        Config.STRAGGLER_PROBABILITY = 0.001

# Return the bandwidth multiplier from physical node1 to node2
# For now, assume uniform bandwidth
def bandwidth(node1, node2):
    return Config.BANDWIDTH_MULTIPLIER


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
    if random.random() < Config.STRAGGLER_PROBABILITY:
        return Config.STRAGGLER_LENGTH_MULTIPLIER * size
    return 0

# A function that returns a list of physical nodes that fail in the
# current timestep. At every timestep, each physical node may fail with
# probability FAILURE_PROBABILITY (node failures are independent). If the
# total execution time is t, the fraction of physical nodes that will fail is
# 1 - (1 - FAILURE_PROBABILITY) ** t.
# For example, if FAILURE_PROBABILITY = 0.001 and t = 100, then approximately
# 10% of the physical nodes will fail at some point.
def failure(pnodes):
    return [pn for pn in pnodes
            if (not pn.failed) and random.random() < Config.FAILURE_PROBABILITY]

# Default functions for computation time and output size (just the size for now)

def default_comp_length(size):
    return Config.COMP_LENGTH_MULTIPLIER * size + straggler_time(size)

def comp_length_with_straggler(size):
    return Config.COMP_LENGTH_MULTIPLIER * size + straggler_time(size)

def default_output_length(size):
    return Config.OUTPUT_LENGTH_MULTIPLIER * size

class LogicalNode:
    lnode_count = 0
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_length=default_output_length,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED,
                type=LogicalNodeType.OTHER,
                id = None):
        self.id = id
        if id is None:
            self.id = 'lnode_' + str(LogicalNode.lnode_count)
            LogicalNode.lnode_count += 1
        self.comp_length = comp_length
        self.output_length = output_length
        self.pnode = pnode
        self.input_q = input_q if input_q is not None else []
        self.in_neighbors = in_neighbors if in_neighbors is not None else []
        self.out_neighbors = out_neighbors if out_neighbors is not None else []
        self.state = state
        self.type = type
        self.schedule_time = None
        self.comp_start_time = None
        self.comp_end_time = None

    @property
    def input_size(self):
        return sum([x.size for x in self.input_q])

    @property
    def ninputs(self):
        return len(self.in_neighbors)

    @property
    def comp_time(self):
        return self.comp_length(self.input_size)

    @property
    def output_size(self):
        return self.output_length(self.input_size)

    # Can this logical node be scheduled?
    def schedulable(self):
        return (self.state is LogicalNodeState.NOT_SCHEDULED or self.state is LogicalNodeState.FAILED) and self.inputs_present()

    # Are all the inputs present for this logical node (not necessarily arrived)
    def inputs_present(self):
        return len(self.input_q) == self.ninputs

class MapNode(LogicalNode):
    map_count = 0
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_length=default_output_length,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        nid = 'map_' + str(MapNode.map_count)
        MapNode.map_count += 1
        super().__init__(ninputs, pnode, input_q, comp_length, output_length, in_neighbors, out_neighbors, state, LogicalNodeType.MAP, nid)

    @property
    def ninputs(self):
        return 1

class ReduceNode(LogicalNode):
    reduce_count = 0
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_length=default_output_length,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        nid = 'reduce_' + str(ReduceNode.reduce_count)
        ReduceNode.reduce_count += 1
        super().__init__(ninputs, pnode, input_q, comp_length, output_length, in_neighbors, out_neighbors, state, LogicalNodeType.REDUCE, nid)

class ShuffleNode(LogicalNode):
    shuffle_count = 0
    def __init__(self, ninputs=None, pnode=None, input_q=None,
                comp_length=default_comp_length,
                output_length=default_output_length,
                in_neighbors=None, out_neighbors=None,
                state=LogicalNodeState.NOT_SCHEDULED):
        nid = 'shuffle_' + str(ShuffleNode.shuffle_count)
        ShuffleNode.shuffle_count += 1
        super().__init__(ninputs, pnode, input_q, comp_length, output_length, in_neighbors, out_neighbors, state, LogicalNodeType.SHUFFLE, nid)

    def schedulable(self):
        return (self.state is LogicalNodeState.NOT_SCHEDULED or self.state is LogicalNodeState.FAILED) and len(self.input_q) > 0



class PhysicalNode:
    pnode_count = 0
    def __init__(self, compute_power=None, memory=None,
                bandwidth=None, lnode=None, failed=False):
        self.id = 'pnode_' + str(PhysicalNode.pnode_count)
        PhysicalNode.pnode_count += 1
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
        self.timestamp = timer.delta(self.size * bandwidth(self.source, pnode))
