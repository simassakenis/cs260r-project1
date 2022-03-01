# Implemented as a class for cross-file imports (and so it can be abstracted)
class Timer:
    def __init__(self):
        self.time = 0

    # Step forward once in time
    def time_step(self):
        self.time += 1

    # Has `time` already passed?
    def time_passed(self, time):
        return time <= self.time

    # Return the current timestamp plus `delta` time (will be useful later to
    # implement jumps forward)
    def time_delta(self, delta):
        end = self.time + delta
        # if end < self.interesting_time:
        #    self.interesting_time = end
        return end

    # Get the current time
    def get_time(self):
        return self.time