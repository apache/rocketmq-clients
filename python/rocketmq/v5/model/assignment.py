from rocketmq.v5.model import MessageQueue


class Assignment:

    def __init__(self, message_queue):
        self.__message_queue = message_queue

    def __str__(self):
        return str(self.__message_queue)

    def __eq__(self, other):
        return self.__message_queue == other.message_queue

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Assignment):
            return NotImplemented
        return self.__message_queue < other.message_queue

    @property
    def message_queue(self):
        return self.__message_queue


class Assignments:

    def __init__(self, assignments):
        self.__assignments = list(
            map(lambda assignment: Assignment(MessageQueue(assignment.message_queue)), assignments)
        )

    def __str__(self):
        if not self.__assignments:
            return "None"
        assignment_strs = ", ".join(str(assignment) for assignment in self.__assignments)
        return f"{assignment_strs}"

    def __eq__(self, other):
        return sorted(self.__assignments) == sorted(other.assignments)

    @staticmethod
    def diff_queues(left, right):
        return [queue for queue in left.message_queues() if queue not in set(right.message_queues())]

    @property
    def assignments(self):
        return self.__assignments

    def message_queues(self):
        return list(
            map(lambda assignment: assignment.message_queue, self.__assignments)
        )
