import os

from bottle import Bottle, request, run

from db import DB
from encryption import Encryption


class Producer:
    producer_api_url = 'http://localhost:8080'
    task_declaration = {
        'workers_needed': 1,
        'producer_api_url': producer_api_url,
    }
    task_details = {
        'task': '2+2',
    }
    workers_found = 0
    task_assigned = False
    key_fn = 'keys/producer.pem'

    def __init__(self):
        self.db = DB()
        self.e = Encryption()

        d = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(d, self.key_fn)
        self.e.import_key(path)
        self.public_key_str = self.e.get_public_key().decode()

    def create_task_declaration(self):
        self.task_declaration_asset_id = self.db.create_asset(
            'Task declaration',
            self.task_declaration
        )
        print('Created task declaration {}'.format(
            self.task_declaration_asset_id
        ))

    def ready(self):
        if self.task_assigned:
            return {'status': 'ok', 'msg': 'Already assigned.'}
        self.workers_found += 1
        if self.workers_found == self.task_declaration['workers_needed']:
            task_assignment = {
                'worker': request.json['worker'],
                'task': self.e.encrypt(
                    self.task_details['task'].encode(),
                    request.json['public_key'],
                ).decode(),
                'public_key': self.public_key_str,
            }
            self.task_assignment_asset_id = self.db.create_asset(
                'Task assignment',
                task_assignment
            )
            self.task_assigned = True
            print('Created task assignment {}'.format(
                self.task_assignment_asset_id
            ))
        return {'status': 'ok'}

if __name__ == '__main__':
    p = Producer()
    p.create_task_declaration()

    bottle = Bottle()
    bottle.post('/ready/')(p.ready)
    run(bottle, host='localhost', port=8080)
