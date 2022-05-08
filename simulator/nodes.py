from simulator.timer import Timer
from enum import Enum
import random

# Constants that should later be configurable

class Cluster:
    def __init__(self, pnodes, bandwidth, latency):
        self.physical_nodes = pnodes
        self.bandwidth = bandwidth
        self.latency = latency

    def get_bandwidth(self, pnode1, pnode2):
        if (pnode1, pnode2) in self.bandwidth:
            return self.bandwidth[(pnode1, pnode2)]
        else:
            return self.bandwidth[(pnode2, pnode1)]
    
    def get_latency(self, pnode1, pnode2):
        return self.latency[(pnode1, pnode2)]

    @staticmethod
    def default_cluster():
        pnodes=[PhysicalNode(compute_power=1, memory=10) for i in range(0, 3)]
        bandwidth = {}
        latency = {}
        for pnode1 in pnodes:
            for pnode2 in pnodes:
                bandwidth[(pnode1, pnode2)] = float("inf") if pnode1 == pnode2 else 1
                latency[(pnode1, pnode2)] = 0 if pnode1 == pnode2 else 0.1
        return Cluster(pnodes, bandwidth, latency)

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
# Latency is 0 from a node to itself
def bandwidth(node1, node2):
    return Config.BANDWIDTH_MULTIPLIER if node1 is not node2 else float("inf")


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
        # print('st: ', Config.STRAGGLER_LENGTH_MULTIPLIER * size)
        return Config.STRAGGLER_LENGTH_MULTIPLIER * size
    return 0

# A function that returns a list of physical nodes that failed in the time
# interval `time`. At every timestep, each physical node may fail with
# probability FAILURE_PROBABILITY (node failures are independent). If the
# total execution time is t, the fraction of physical nodes that will fail is
# 1 - (1 - FAILURE_PROBABILITY) ** t.
# For example, if FAILURE_PROBABILITY = 0.001 and t = 100, then approximately
# 10% of the physical nodes will fail at some point.
# TODO actually use `time`; I believe it may be random.random() <
# (1 - Config.FAILURE_PROBABILITY) ** time, but I'm not sure
def failure(pnodes, time):
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
        self.max_input_time = -1

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

    def update_max(self, timestamp):
        if timestamp > self.max_input_time:
            self.max_input_time = timestamp

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
                 lnode=None, failed=False):
        self.id = 'pnode_' + str(PhysicalNode.pnode_count)
        PhysicalNode.pnode_count += 1
        self.compute_power = compute_power
        self.memory = memory
        self.lnode = lnode
        self.failed = failed

    # Can this physical node be scheduled?
    def schedulable(self):
        return self.lnode is None and not self.failed

class Input:
    def __init__(self, size=None, timestamp=None, from_lnode=None, to_lnode=None):
        self.size = size
        self.timestamp = timestamp
        self.from_lnode = from_lnode
        self.to_lnode = to_lnode
        self.from_pnode = None
        self.to_pnode = None

    def set_from_pnode(self, pnode):
        self.from_pnode = pnode
    
    def set_to_pnode(self, pnode):
        self.to_pnode = pnode

    # Update the timestamp of this input to arrive at the physical node `pnode`
    def update_time(self, timer, cluster):
        time = max(self.size / cluster.get_bandwidth(self.from_pnode, self.to_pnode),
                   cluster.get_latency(self.from_pnode, self.to_pnode))
        self.timestamp = timer.delta(time)
