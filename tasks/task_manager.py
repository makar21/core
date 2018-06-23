import json

from .task import Task

class TaskManager:
    def __init__(self,
                 db,
                 ipfs,
                 encryption):
        self.db = db
        self.ipfs = ipfs
        self.encryption = encryption

    def pick_worker_task(self):
        asset_ids = self.db.retrieve_created_asset_ids('Task declaration')
        for asset_id in asset_ids:
            asset = self.db.retrieve_asset(asset_id)
            task_dict = json.loads(
                self.encryption.decrypt(asset.data['task'].encode())
            )
            if asset.data['workers_needed'] > 0:
                return Task(
                    ipfs=self.ipfs,
                    encryption=self.encryption,
                    producer_id=asset.data['producer_id'],
                    task=task_dict['task'],
                    args=task_dict['args'],
                    workers_needed=asset.data['workers_needed'],
                )
