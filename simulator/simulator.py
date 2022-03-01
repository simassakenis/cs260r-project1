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

lnode_count = 0
pnode_count = 0

BANDWIDTH_MULTIPLIER = 1
COMP_LENGTH_MULTIPLIER = 1
OUTPUT_SIZE_MULTIPLIER = 1

# Logical Node state enum
class LogicalNodeState(Enum):
    NOT_SCHEDULED = 1
    NEED_INPUT = 2
    COMPUTING = 3
    COMPLETED = 4
    FAILED = 5

# Return the bandwidth multiplier from physical node1 to node2
# For now, assume uniform bandwidth
def bandwidth(node1, node2):
    global BANDWIDTH_MULTIPLIER
    return BANDWIDTH_MULTIPLIER

# Extra time to add to a logical node as a straggler (for now just a small
# random chance for every node, but we could make it specific to certain sizes,
# etc)
def straggler_time(size):
    global COMP_LENGTH_MULTIPLIER
    if random() < (1/1000):
        return COMP_LENGTH_MULTIPLIER * size
    return 0

# Default functions for computation time and output size (just the size for now)

def default_comp_length(size):
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
                 state=LogicalNodeState.NOT_SCHEDULED):
        global lnode_count
        self.node_id = lnode_count
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

class PhysicalNode:
    def __init__(self, compute_power=None, memory=None,
                bandwidth=None, lnode=None, failed=False):
        global pnode_count
        self.node_id = pnode_count
        pnode_count += 1
        self.compute_power = compute_power
        self.memory = memory
        self.bandwidth = bandwidth
        self.lnode = lnode
        self.failed = failed

timestamp = 0

# Return the current timestamp plus delta time (will be useful later to
# implement jumps forward)
def time_delta(delta):
    global timestamp
    end = timestamp + delta
    # if end < interesting_time:
    #    interesting_time = end
    return end

class Input:
    def __init__(self, size=None, timestamp=None, source=None):
        self.size = size
        self.timestamp = timestamp
        self.source = source
    # Update the timestamp of this input to arrive at the physical node pnode
    def update_time(pnode):
        self.timestamp = time_delta(self.size * bandwidth(self.source, pnode))

def scheduler(logical_nodes, physical_nodes):
    #TODO
    pass


def failure(logical_nodes):
    #TODO
    pass

def simulator(lnodes, pnodes):
    global timestamp
    while True:
        node_assignments = scheduler(lnodes, pnodes)
        for lnode, pnode in node_assignments:
            assert lnode.state == NOT_SCHEDULED
            assert pnode.lnode == None
            assert not pnode.failed
            lnode.pnode = pnode
            pnode.lnode = lnode
            for inp in lnode.input_q:
                if inp.timestamp == None:
                    inp.update_time(pnode)
            lnode.state = NEED_INPUT

        for lnode in lnodes:
            done = True
            if lnode.state == NEED_INPUT:
                if (len(lnode.input_q) == lnode.ninputs and
                    max([inp.timestamp for inp in lnode.input_q]) <= timestamp):
                    lnode.input_size = sum([inp.size for inp in lnode.input_q])
                    lnode.comp_finish_time = time_delta(lnode.comp_length(lnode.input_size))
                    lnode.state = COMPUTING

            if lnode.state == COMPUTING:
                if lnode.comp_finish_time <= timestamp:
                    for node in lnode.out_neighbors:
                        inp = Input(lnode.output_size(lnode.input_size), None, lnode.pnode)
                        if node.pnode is not None:
                            inp.update_time(node.pnode)
                        node.input_q.push(inp)
                    lnode.pnode.lnode = None
                    lnode.state = COMPLETED

            if lnode.state != COMPLETED:
                done = False

        if done:
            return timestamp

        failed_nodes = failure(pnodes)
        for pnode in failed_nodes:
            if pnode.lnode is not None:
                pnode.lnode.state = FAILED
            pnode.failed = True

        timestamp += 1


# if __name__ == '__main__':
#     logical_nodes = #TODO (create logical graph)
#     execution_time = simulator(logical_nodes)
