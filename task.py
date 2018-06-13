import time


class Task:
    def __init__(self, producer_id, task, workers_needed=1):
        self.producer_id = producer_id
        self.task = task
        self.workers_needed = workers_needed
        self.timestamp = int(time.time())
        self.workers_found = 0
        self.assigned = False

    @property
    def task_declaration(self):
        return {
            'workers_needed': self.workers_needed,
            'timestamp': self.timestamp,
            'producer_id': self.producer_id,
        }