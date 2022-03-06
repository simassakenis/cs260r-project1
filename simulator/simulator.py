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

from simulator.nodes import LogicalNode, PhysicalNode, Input, LogicalNodeState, LogicalNodeType, MapNode, ReduceNode, ShuffleNode, failure
from simulator.timer import Timer

def simulate(lnodes, pnodes, scheduler_class, verbose=True):
    fail_count = 0
    timer = Timer()
    completed_lnodes = []
    failed_lnodes = []
    while True:
        if verbose:
            print('Current time: {}'.format(timer.now()))
        node_assignments = scheduler_class.schedule(lnodes, pnodes, completed_lnodes, failed_lnodes)
        completed_lnodes.clear()
        failed_lnodes.clear()
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
                    completed_lnodes.append(lnode)

            if lnode.state != LogicalNodeState.COMPLETED:
                done = False

        if done:
            if verbose:
                print('total fails: {}'.format(fail_count))
            return timer.now()

        failed_nodes = failure(pnodes, timer.elapsed())
        for pnode in failed_nodes:
            if pnode.failed:
                continue
            if pnode.lnode is not None:
                for inp in pnode.lnode.input_q:
                    inp.timestamp = None
                pnode.lnode.state = LogicalNodeState.FAILED
                failed_lnodes.append(lnode)
            pnode.failed = True
            fail_count += 1
            if verbose:
                print('{} failed.'.format(pnode.id))
                if(pnode.lnode is not None):
                    print('{} aborted'.format(pnode.lnode.id))

        # find all running nodes
        running_nodes = list(filter(lambda x: x.state is LogicalNodeState.COMPUTING or x.state is LogicalNodeState.NEED_INPUT, lnodes))
        m_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.MAP, running_nodes)))
        s_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.SHUFFLE, running_nodes)))
        r_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.REDUCE, running_nodes)))

        print('{}, {}, {}'.format(m_running_nodes, s_running_nodes, r_running_nodes))

        timer.step(len(failed_lnodes) > 0 or len(completed_lnodes) > 0)
