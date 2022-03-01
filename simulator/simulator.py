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
from simulator.nodes import LogicalNode, PhysicalNode, Input, LogicalNodeState, PhysicalNodeState
import logging


def scheduler(logical_nodes, physical_nodes):
    #TODO
    return SimpleQueueScheduler.schedule(logical_nodes, physical_nodes)


def failure(logical_nodes):
    #TODO
    return []


def simulate(logical_nodes, physical_nodes):
    '''
        
    '''
    current_timestamp = 0
    while True:
        print('Current timestamp: ', current_timestamp)
        new_logical_to_physical_assignments = SimpleQueueScheduler.schedule(logical_nodes, physical_nodes)
        for logical_node, physical_node in new_logical_to_physical_assignments:
            assert logical_node.state is LogicalNodeState.NOT_SCHEDULED
            print('Assigning logical node {} to physical node {}. Waiting for inputs'.format(logical_node.node_id, physical_node.node_id))
            logical_node.phys_node = physical_node
            # TODO: update input_q timestamps
            for inp in logical_node.input_q:
                inp.timestamp = current_timestamp + int(inp.data_size / physical_node.bandwidth)
            logical_node.state = LogicalNodeState.WAITING_FOR_INPUTS
            physical_node.current_logical_node = logical_node
            physical_node.state = PhysicalNodeState.COMPUTING

        for logical_node in logical_nodes:
            if logical_node.state is LogicalNodeState.WAITING_FOR_INPUTS:
                if (len(logical_node.input_q) == logical_node.number_of_inputs
                    and max([inp.timestamp for inp in logical_node.input_q]) <= current_timestamp):
                    print('Logical node {} received all inputs and is computing.'.format(logical_node.node_id))
                    logical_node.state = LogicalNodeState.COMPUTING
                    input_sz = sum([inp.data_size for inp in logical_node.input_q])
                    logical_node.computation_timestamp = (current_timestamp
                        + logical_node.computation_length)

            if logical_node.state is LogicalNodeState.COMPUTING:
                if logical_node.computation_timestamp == current_timestamp:
                    print('Logical node {} is done computing'.format(logical_node.node_id))
                    logical_node.state = LogicalNodeState.COMPLETED
                    physical_node.state = PhysicalNodeState.NOT_SCHEDULED
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
