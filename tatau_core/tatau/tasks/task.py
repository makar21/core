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
                metadata=self.get_metadata(),
                recipients=recipients,
            )
        else:
            raise ValueError('Use add method for create new task')

    def get_data(self):
        return {
            'name': self.task_type
        }

    def get_metadata(self):
        raise NotImplemented

    def to_json(self):
        return json.dumps(self.get_data())

    @classmethod
    def list(cls, node, additional_match=None, created_by_user=True):
        node.db.connect_to_mongodb()
        match = {
            'assets.data.name': cls.task_type,
        }

        if additional_match is not None:
            match.update(additional_match)
        return (cls.get(node, x) for x in node.db.retrieve_asset_ids(match=match, created_by_user=created_by_user))

    @classmethod
    def exists(cls, node, additional_match=None, created_by_user=True):
        for v in cls.list(node, additional_match, created_by_user):
            return True
        return False
