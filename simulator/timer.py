# Implemented as a class for cross-file imports (and so it can be abstracted)
class Timer:
    def __init__(self):
        self.time = 0

    # Step forward once in time
    def step(self):
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
            return time - self.time
        return 0
