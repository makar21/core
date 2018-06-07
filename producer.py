import time

from bottle import Bottle, request, run

from db import DB
from encryption import Encryption


class Producer:
    producer_api_url = 'http://localhost:8080'
    task_declaration = {
        'workers_needed': 1,
        'timestamp': int(time.time())
    }
    task_details = {
        'task': '2+2',
    }
    workers_found = 0
    task_assigned = False

    def __init__(self):
        self.db = DB('producer')
        self.e = Encryption('producer')

        self.producer_id = self.db.create_asset('Producer info', {
            'enc_key': self.e.get_public_key().decode(),
            'producer_api_url': self.producer_api_url,
        })

        self.task_declaration['producer_id'] = self.producer_id

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
            worker_id = request.json['worker']
            worker_info = self.db.retrieve_asset(worker_id)
            task_assignment = {
                'worker': worker_id,
                'task': self.e.encrypt(
                    self.task_details['task'].encode(),
                    worker_info['enc_key'],
                ).decode(),
                'producer_id': self.producer_id,
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
