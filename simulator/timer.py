# Implemented as a class for cross-file imports (and so it can be abstracted)
class Timer:
    def __init__(self):
        self.time = 0

    # Step forward in time - if `single`, then require the step be a single
    # step (helpful to ensure something, like scheduling, happens right away)
    def step(self, single):
        self.time += 1

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
