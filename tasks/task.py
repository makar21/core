import json
import time


class Task:
    def __init__(self,
                 db,
                 ipfs,
                 encryption,
                 producer_id,
                 td_asset_id=None,
                 vd_asset_id=None,
                 ta_asset_id=None,
                 task=None,
                 args=(),
                 workers_needed=None,
                 verifiers_needed=None):
        self.db = db
        self.ipfs = ipfs
        self.encryption = encryption

        self.producer_id = producer_id
        self.td_asset_id = td_asset_id
        self.vd_asset_id = vd_asset_id
        self.ta_asset_id = ta_asset_id
        self.task = task
        self.args = args
        self.workers_needed = workers_needed
        self.verifiers_needed = verifiers_needed
        self.timestamp = int(time.time())

    def upload_to_ipfs(self, task):
        ipfs_file = self.ipfs.add_file(task)
        self.task = ipfs_file.multihash

    def save_task_declaration(self):
        if self.td_asset_id:
            self.db.update_asset(
                asset_id=self.td_asset_id,
                data=self.task_declaration,
            )
        else:
            self.td_asset_id = self.db.create_asset(
                name='Task declaration',
                data=self.task_declaration,
            )

    def save_verification_declaration(self):
        if self.vd_asset_id:
            self.db.update_asset(
                asset_id=self.vd_asset_id,
                data=self.verification_declaration,
            )
        else:
            self.vd_asset_id = self.db.create_asset(
                name='Verification declaration',
                data=self.verification_declaration,
            )

    def create_task_assignment(self, worker_id):
        worker_info = self.db.retrieve_asset(worker_id)

        task_assignment = {
            'worker': worker_id,
            'task': self.encryption.encrypt(
                self.json_str.encode(),
                worker_info.data['enc_key'],
            ).decode(),
            'producer_id': self.producer_id,
            'td_asset_id': self.td_asset_id,
        }

        asset_id = self.db.create_asset(
            name='Task assignment',
            data=task_assignment,
            recipients=worker_info.tx['outputs'][0]['public_keys'][0],
        )

        return asset_id

    def create_verification_assignment(self, verifier_id, result):
        verifier_info = self.db.retrieve_asset(verifier_id)

        decrypted_result = self.encryption.decrypt(result).decode()

        verification_assignment = {
            'verifier': verifier_id,
            'task': self.encryption.encrypt(
                self.json_str.encode(),
                verifier_info.data['enc_key'],
            ).decode(),
            'result': self.encryption.encrypt(
                decrypted_result.encode(),
                verifier_info.data['enc_key'],
            ).decode(),
            'producer_id': self.producer_id,
        }

        asset_id = self.db.create_asset(
            name='Verification assignment',
            data=verification_assignment,
            recipients=verifier_info.tx['outputs'][0]['public_keys'][0],
        )

        return asset_id

    @property
    def task_declaration(self):
        return {
            'workers_needed': self.workers_needed,
            'timestamp': self.timestamp,
            'producer_id': self.producer_id,
            'task': self.encryption.encrypt(
                self.json_str.encode(),
                self.encryption.get_public_key(),
            ).decode(),
        }

    @property
    def verification_declaration(self):
        return {
            'verifiers_needed': self.verifiers_needed,
            'timestamp': self.timestamp,
            'producer_id': self.producer_id,
            'td_asset_id': self.td_asset_id,
            'ta_asset_id': self.ta_asset_id,
        }

    @property
    def json_str(self):
        return json.dumps({
            'task': self.task,
            'args': self.args,
        })
