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
from simulator.nodes import LogicalNode, PhysicalNode, Input, LogicalNodeState
from simulator.timer import Timer
import logging


def scheduler(lnodes, pnodes):
    return SimpleQueueScheduler.schedule(lnodes, pnodes)

def failure(pnodes):
    #TODO
    return []

def simulate(lnodes, pnodes):
    timer = Timer()
    while True:
        print('Current time: ', timer.get_time())
        node_assignments = scheduler(lnodes, pnodes)
        for lnode, pnode in node_assignments:
            assert lnode.schedulable()
            assert pnode.schedulable()
            print('Assigned logical node {} to physical node {}; now waiting'
                  .format(lnode.id, pnode.id))
            lnode.pnode = pnode
            pnode.lnode = lnode
            for inp in lnode.input_q:
                if inp.timestamp == None:
                    inp.update_time(timer, pnode)
            lnode.state = LogicalNodeState.NEED_INPUT

        for lnode in lnodes:
            done = True
            if lnode.state is LogicalNodeState.NEED_INPUT:
                if (lnode.inputs_present() and
                    all([timer.time_passed(inp.timestamp) for inp in lnode.input_q])):
                    print('Logical node {} now computing'.format(lnode.id))
                    lnode.input_size = sum([inp.size for inp in lnode.input_q])
                    lnode.comp_finish_time = timer.time_delta(lnode.comp_length(lnode.input_size))
                    lnode.state = LogicalNodeState.COMPUTING

            if lnode.state is LogicalNodeState.COMPUTING:
                if timer.time_passed(lnode.comp_finish_time):
                    print('Logical node {} finished computing'.format(lnode.id))
                    for node in lnode.out_neighbors:
                        inp = Input(lnode.output_size(lnode.input_size), None, lnode.pnode)
                        if node.pnode is not None:
                            inp.update_time(timer, node.pnode)
                        node.input_q.append(inp)
                    lnode.pnode.lnode = None
                    lnode.state = LogicalNodeState.COMPLETED

            if lnode.state != LogicalNodeState.COMPLETED:
                done = False

        if done:
            return timer.get_time()

        failed_nodes = failure(pnodes)
        for pnode in failed_nodes:
            if pnode.lnode is not None:
                pnode.lnode.state = LogicalNodeState.FAILED
            pnode.failed = True

        timer.time_step()


# if __name__ == '__main__':
#     lnodes = # TODO (create logical graph)
#     pnodes = # TODO (make physical node list)
#     execution_time = simulate(lnodes, pnodes)
