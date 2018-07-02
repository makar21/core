from __future__ import absolute_import

import json
import time


class Task:
    class TaskType:
        TASK_DECLARATION = 'Task declaration'
        TASK_ASSIGNMENT = 'Task assignment'
        VERIFICATION_DECLARATION = 'Verification declaration'
        VERIFICATION_ASSIGNMENT = 'Verification assignment'

    task_type = None

    def __init__(self, asset_id, *args, **kwargs):
        self.asset_id = asset_id
        self.timestamp = int(time.time())

    @classmethod
    def add(cls, node, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def get(cls, node, asset_id):
        raise NotImplemented

    def save(self, db, recipients=None):
        if self.asset_id is not None:
            db.update_asset(
                asset_id=self.asset_id,
                data=self.get_data(),
                recipients=recipients,
                sleep=True
            )
        else:
            raise ValueError('Use add method for create new task')

    def get_data(self):
        raise NotImplemented

    def to_json(self):
        return json.dumps(self.get_data())


