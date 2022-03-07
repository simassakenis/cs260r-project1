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

from simplequeuescheduler import SimpleQueueScheduler
from mrscheduler import MRScheduler
from nodes import LogicalNode, PhysicalNode, Input, LogicalNodeState, LogicalNodeType, MapNode, ReduceNode, ShuffleNode, failure
from timer import Timer
import logging
import random
import numpy as np
import matplotlib.pyplot as plt; plt.close('all')
import networkx as nx
from matplotlib.animation import FuncAnimation


def iterate(timer, lnodes, pnodes, scheduler_class, verbose=True):
    done_ = False
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
        done_ = True
        return timer.now(), done_

    failed_nodes = failure(pnodes)
    for pnode in failed_nodes:
        if pnode.lnode is not None:
            for inp in pnode.lnode.input_q:
                inp.timestamp = None
            pnode.lnode.state = LogicalNodeState.FAILED
        pnode.failed = True

    timer.step()
    return timer.now(), done_


def simulate(lnodes, pnodes, scheduler_class, verbose=True):
    timer = Timer()
    done_ = False
    node_colors = [[] for node in pnodes]
    time_steps = 0
    while not done_:
        time, done_ = iterate(timer, lnodes, pnodes, scheduler_class, verbose=True)
        for i, node in enumerate(pnodes):
            if node.lnode:
                state = node.lnode.state
                node_colors[i].append(state.value * 15)
            elif node.failed:
                node_colors[i].append(200)
            else:
                node_colors[i].append(0)
        time_steps += 1

    return node_colors, time_steps


def animate(G, node_colors, pos=None, *args, **kwargs):

    if pos is None:
        pos = nx.spring_layout(G)

    # draw graph
    nodes = nx.draw_networkx_nodes(G, pos, *args, **kwargs)
    edges = nx.draw_networkx_edges(G, pos, *args, **kwargs)
    plt.axis('off')

    def update(ii):
        # nodes are just markers returned by plt.scatter;
        # node color can hence be changed in the same way like marker colors
        print(node_colors)
        nodes.set_array(node_colors[ii])
        return nodes,

    fig = plt.gcf()
    animation = FuncAnimation(fig, update, interval=3, frames=len(node_colors), blit=True)
    return animation



if __name__ == '__main__':
    num_map_nodes = 1
    num_reduce_nodes = 1
    map_computation_length = 1
    reduce_computation_length = 1
    map_node = MapNode(ninputs = 1, input_q = [Input(1,0, None)])
    reduce_node = ReduceNode(ninputs = 1)

    # connecting map node to reduce node
    map_node.out_neighbors.append(reduce_node)
    reduce_node.in_neighbors.append(map_node)

    logical_nodes = [map_node, reduce_node]
    # creating physical node
    physical_node = PhysicalNode(compute_power = 1, memory = 1, bandwidth = 1)
    node_colors, time_steps = simulate(logical_nodes, [physical_node], SimpleQueueScheduler)
    print("Total time: ", time_steps)


    total_nodes = len([physical_node])
    graph = nx.complete_graph(total_nodes)

    node_colors.append(np.zeros(time_steps))
    node_colors = np.array(node_colors)
    node_colors = node_colors.T

    print(node_colors)

    animation = animate(graph, node_colors)
    animation.save('test.gif', writer='imagemagick', savefig_kwargs={'facecolor':'white'}, fps=1)
