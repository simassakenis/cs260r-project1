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

from simulator.nodes import LogicalNode, PhysicalNode, Input, LogicalNodeState, LogicalNodeType, MapNode, ReduceNode, ShuffleNode, failure, Cluster
from simulator.timer import Timer

def simulate(lnodes, pnodes, scheduler_class, cluster=Cluster.default_cluster(), verbose=True):
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
                # if the timestamp is none, then it is just being scheduled. 
                if inp.timestamp == None:
                    inp.set_to_pnode(pnode)
                    inp.update_time(timer, cluster)
                    lnode.update_max(inp.timestamp)
                timer.add_to_queue(inp.timestamp)
            timer.add_to_queue(lnode.max_input_time)
            lnode.state = LogicalNodeState.NEED_INPUT

        for lnode in lnodes:
            done = True
            if lnode.state is LogicalNodeState.NEED_INPUT:
                if (lnode.inputs_present() and timer.passed(lnode.max_input_time)):
                    if verbose:
                        print('{} now computing'.format(lnode.id))
                    if lnode.type is LogicalNodeType.SHUFFLE:
                        running_time = timer.elapsed_since(lnode.schedule_time)
                        remaining_computation_time = max(lnode.comp_time - running_time, 0)
                        lnode.comp_end_time = timer.delta(remaining_computation_time)
                    else:
                        lnode.comp_end_time = timer.delta(lnode.comp_time)
                    timer.add_to_queue(lnode.comp_end_time)
                    lnode.comp_start_time = timer.now()
                    lnode.state = LogicalNodeState.COMPUTING

            if lnode.state is LogicalNodeState.COMPUTING:
                if timer.passed(lnode.comp_end_time):
                    if verbose:
                        print('{} finished computing'.format(lnode.id))
                    for node in lnode.out_neighbors:
                        # for each of the out neighbors of the current node,
                        # add the current node's output to the input queue of the neighbor
                        inp = Input(lnode.output_size, None, lnode, node)
                        inp.set_from_pnode(lnode.pnode)

                        # if the out_neighbor is already scheduled, then set the input's timestamp for transmission
                        if node.pnode is not None:
                            inp.set_to_pnode(node.pnode)
                            inp.update_time(timer, cluster)
                            node.update_max(inp.timestamp)
                            timer.add_to_queue(inp.timestamp)
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
        # running_nodes = list(filter(lambda x: x.state is LogicalNodeState.COMPUTING or x.state is LogicalNodeState.NEED_INPUT, lnodes))
        # m_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.MAP, running_nodes)))
        # s_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.SHUFFLE, running_nodes)))
        # r_running_nodes = len(list(filter(lambda x: x.type is LogicalNodeType.REDUCE, running_nodes)))

        # print('{}, {}, {}'.format(m_running_nodes, s_running_nodes, r_running_nodes))
        if verbose:
            print('next time step: {}'.format(timer.get_next_time()))
        timer.forward_to_next_time_in_queue()
