from enum import Enum

class State(Enum):
    New = 1
    Starting = 2
    Running = 3
    Stopping = 4
    Terminated = 5
    Failed = 6