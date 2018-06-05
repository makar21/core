from bottle import Bottle, request, run

from db import DB


class Producer:
    producer_api_url = 'http://localhost:8080'
    task_declaration = {
        'workers_needed': 1,
        'producer_api_url': producer_api_url,
    }
    task_assignment = {
        'task': '2+2',
    }
    workers_found = 0
    task_assigned = False

    def __init__(self):
        self.db = DB()

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
            self.task_assignment['worker'] = request.json['worker']
            self.task_assignment_asset_id = self.db.create_asset(
                'Task assignment',
                self.task_assignment
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
