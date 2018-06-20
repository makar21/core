import json
import time


class Task:
    def __init__(self,
                 ipfs,
                 producer_id,
                 task,
                 args=(),
                 workers_needed=1,
                 verifiers_needed=1):
        self.ipfs = ipfs

        self.producer_id = producer_id
        self.task = task
        self.args = args
        self.workers_needed = workers_needed
        self.verifiers_needed = verifiers_needed
        self.timestamp = int(time.time())
        self.workers_found = 0
        self.verifiers_found = 0
        self.assigned = False

    def upload_to_ipfs(self):
        self.ipfs_file = self.ipfs.add_file(self.task)

    @property
    def task_declaration(self):
        return {
            'workers_needed': self.workers_needed,
            'timestamp': self.timestamp,
            'producer_id': self.producer_id,
        }

    @property
    def verification_declaration(self):
        return {
            'verifiers_needed': self.verifiers_needed,
            'timestamp': self.timestamp,
            'producer_id': self.producer_id,
        }

    @property
    def json_str(self):
        return json.dumps({
            'task': self.ipfs_file.multihash,
            'args': self.args,
        })
