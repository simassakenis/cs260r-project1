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

from simulator.simplequeuescheduler import SimpleQueueScheduler
from simulator.mrscheduler import MRScheduler
from simulator.nodes import LogicalNode, PhysicalNode, Input, LogicalNodeState, LogicalNodeType, MapNode, ReduceNode, ShuffleNode, failure
from simulator.timer import Timer
import logging

def simulate(lnodes, pnodes, scheduler_class, verbose=True):
    timer = Timer()
    while True:
        if verbose:
            print('Current time: {}'.format(timer.now()))
        node_assignments = scheduler_class.schedule(lnodes, pnodes)
        for lnode, pnode in node_assignments:
            assert lnode.schedulable()
            assert pnode.schedulable()
            if verbose:
                print('Assigned {} to {}; now waiting'.format(lnode.id, pnode.id))
            lnode.pnode = pnode
            pnode.lnode = lnode
            lnode.schedule_time = timer.now()
            for inp in lnode.input_q:
                if inp.timestamp == None:
                    inp.update_time(timer, pnode)
            lnode.state = LogicalNodeState.NEED_INPUT

        for lnode in lnodes:
            done = True
            if lnode.state is LogicalNodeState.NEED_INPUT:
                if (lnode.inputs_present() and all([timer.passed(inp.timestamp) for inp in lnode.input_q])):
                    if verbose:
                        print('{} now computing'.format(lnode.id))
                    if lnode.type is LogicalNodeType.SHUFFLE:
                        running_time = timer.elapsed_since(lnode.schedule_time)
                        remaining_computation_time = max(lnode.comp_time - running_time, 0)
                        lnode.comp_end_time = timer.delta(remaining_computation_time)
                    else:
                        lnode.comp_end_time = timer.delta(lnode.comp_time)
                    lnode.comp_start_time = timer.now()
                    lnode.state = LogicalNodeState.COMPUTING

            if lnode.state is LogicalNodeState.COMPUTING:
                if timer.passed(lnode.comp_end_time):
                    if verbose:
                        print('{} finished computing'.format(lnode.id))
                    for node in lnode.out_neighbors:
                        inp = Input(lnode.output_size, None, lnode.pnode)
                        if node.pnode is not None:
                            inp.update_time(timer, node.pnode)
                        node.input_q.append(inp)
                    lnode.pnode.lnode = None
                    lnode.state = LogicalNodeState.COMPLETED

            if lnode.state != LogicalNodeState.COMPLETED:
                done = False

        if done:
            return timer.now()

        failed_nodes = failure(pnodes)
        for pnode in failed_nodes:
            if pnode.lnode is not None:
                for inp in pnode.lnode.input_q:
                    inp.timestamp = None
                pnode.lnode.state = LogicalNodeState.FAILED
            pnode.failed = True

        timer.step()
