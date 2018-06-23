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

    def get_task(self, asset, vd_asset=None):
        """
        Takes an Asset object as an argument and creates a Task object.

        Optional vs_asset argument can be used to pass
        a verification declaration asset (used for extra data).

        Returns Task object.
        """
        task_dict = json.loads(
            self.encryption.decrypt(asset.data['task'].encode())
        )

        task_dict = {
            'db': self.db,
            'ipfs': self.ipfs,
            'encryption': self.encryption,
            'task': task_dict['task'],
            'args': task_dict['args'],
            'producer_id': asset.data['producer_id'],
            'td_asset_id': asset.asset_id,
            'workers_needed': asset.data['workers_needed'],
        }

        if vd_asset:
            task_dict.update({
                'vd_asset_id': vd_asset.asset_id,
                'verifiers_needed': vd_asset.data['verifiers_needed'],
                'ta_asset_id': vd_asset.data['ta_asset_id'],
            })

        return Task(**task_dict)

    def pick_worker_task(self):
        asset_ids = self.db.retrieve_created_asset_ids('Task declaration')
        for asset_id in asset_ids:
            asset = self.db.retrieve_asset(asset_id)
            if asset.data['workers_needed'] > 0:
                return self.get_task(asset)

    def pick_verifier_task(self):
        asset_ids = self.db.retrieve_created_asset_ids(
            'Verification declaration'
        )
        for asset_id in asset_ids:
            asset = self.db.retrieve_asset(asset_id)
            if asset.data['verifiers_needed'] > 0:
                td_asset = self.db.retrieve_asset(asset.data['td_asset_id'])
                return self.get_task(td_asset, asset)
