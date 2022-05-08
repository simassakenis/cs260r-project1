# Implemented as a class for cross-file imports (and so it can be abstracted)
class Timer:
    def __init__(self):
        self.time = 0
        self.t_queue = set()

    # Step forward in time - if `single`, then require the step be a single
    # step (helpful to ensure something, like scheduling, happens right away)
    def step(self, length=1):
        self.time += length

    # Return the current timestamp plus `delta` time
    def delta(self, delta):
        return self.time + delta

    # Get the current time
    def now(self):
        return self.time

    # Has `time` already passed?
    def passed(self, time):
        return time <= self.time

    # Return the time elapsed since `time`, or 0 if that time has yet to occur
    def elapsed_since(self, time):
        if self.passed(time):
            return self.time - time
        return 0

    # Return the time elapsed since the last call to step
    def elapsed(self):
        return 1
    
    def add_to_queue(self, time):
        if(int(time) != time):
            # time is not an integer, add floor and ceil
            # self.t_queue.add(int(time))
            self.t_queue.add(int(time) + 1)
        else:
            self.t_queue.add(int(time))

    def get_next_time(self):
        if len(self.t_queue) == 0:
            return None
        return min(self.t_queue)
    
    def forward_to_next_time_in_queue(self):
        next_step = self.get_next_time()
        if next_step is None:
            self.step()
        else:
            print('skipping from {} to {}'.format(self.time, next_step))
            self.time = next_step
            self.t_queue.remove(next_step)
