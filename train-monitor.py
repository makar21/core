import argparse
import time
from logging import getLogger, basicConfig, StreamHandler, INFO

from tatau_core.contract import NodeContractInfo
from tatau_core.tatau.models import TaskDeclaration, TaskAssignment, VerificationAssignment
from tatau_core.tatau.node import Producer

from termcolor import colored

basicConfig(
    format='%(message)s',
    level=INFO,
    handlers=[
        StreamHandler()
    ],
)

logger = getLogger(__name__)


def red(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'red', on_color=on_color, attrs=attrs)


def grey(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'grey', on_color=on_color, attrs=attrs)


def green(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'green', on_color=on_color, attrs=attrs)


def yellow(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'yellow', on_color=on_color, attrs=attrs)


def magenta(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'magenta', on_color=on_color, attrs=attrs)


def blue(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'blue', on_color=on_color, attrs=attrs)


def cyan(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'cyan', on_color=on_color, attrs=attrs)


def white(text, on_color=None, attrs=None):
    return colored('{}'.format(text), 'white', on_color=on_color, attrs=attrs)


def get_progress_data(task_declaration):
    data = {
        'asset_id': task_declaration.asset_id,
        'dataset': task_declaration.dataset.name,
        'train_model': task_declaration.train_model.name,
        'state': task_declaration.state,
        'accepted_workers': '{}/{}'.format(
            task_declaration.workers_requested - task_declaration.workers_needed, task_declaration.workers_requested),
        'accepted_verifiers': '{}/{}'.format(
            task_declaration.verifiers_requested - task_declaration.verifiers_needed,
            task_declaration.verifiers_requested),
        'total_progress': task_declaration.progress,
        'current_epoch': task_declaration.current_epoch,
        'epochs': task_declaration.epochs,
        'history': {},
        'spent_tflops': task_declaration.tflops,
        'estimated_tflops': task_declaration.estimated_tflops,
        'workers': {},
        'verifiers': {}
    }

    if task_declaration.state == TaskDeclaration.State.COMPLETED:
        data['train_result'] = task_declaration.weights

    for td in TaskDeclaration.get_history(task_declaration.asset_id):
        if td.loss and td.accuracy and td.state in [TaskDeclaration.State.EPOCH_IN_PROGRESS, TaskDeclaration.State.COMPLETED]:
            if td.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
                epoch = td.current_epoch - 1
            else:
                epoch = td.current_epoch

            data['history'][epoch] = {
                'loss': td.loss,
                'accuracy': td.accuracy
            }

    task_assignments = task_declaration.get_task_assignments(
        states=(TaskAssignment.State.IN_PROGRESS, TaskAssignment.State.DATA_IS_READY, TaskAssignment.State.FINISHED)
    )

    for task_assignment in task_assignments:
        worker_id = task_assignment.worker_id
        data['workers'][worker_id] = []
        history = TaskAssignment.get_history(task_assignment.asset_id)
        for ta in history:
            if ta.state == TaskAssignment.State.FINISHED:
                data['workers'][worker_id].append({
                    'asset_id': ta.worker_id,
                    'state': ta.state,
                    'current_epoch': ta.current_epoch,
                    'progress': ta.progress,
                    'spent_tflops': ta.tflops,
                    'loss': ta.loss,
                    'accuracy': ta.accuracy
                })

        if task_assignment.state != TaskAssignment.State.FINISHED:
            data['workers'][worker_id].append({
                'asset_id': task_assignment.asset_id,
                'state': task_assignment.state,
                'current_epoch': task_assignment.current_epoch,
                'progress': task_assignment.progress,
                'spent_tflops': task_assignment.tflops,
                'loss': task_assignment.loss,
                'accuracy': task_assignment.accuracy
            })

    verification_assignments = task_declaration.get_verification_assignments(
        states=(
            VerificationAssignment.State.IN_PROGRESS,
            VerificationAssignment.State.DATA_IS_READY,
            VerificationAssignment.State.FINISHED
        )
    )

    for verification_assignment in verification_assignments:
        verifier_id = verification_assignment.verifier_id
        data['verifiers'][verifier_id] = []
        history = VerificationAssignment.get_history(verification_assignment.asset_id)
        for va in history:
            if va.state == VerificationAssignment.State.FINISHED:
                data['verifiers'][verifier_id].append({
                    'asset_id': va.verifier_id,
                    'state': va.state,
                    'progress': va.progress,
                    'spent_tflops': va.tflops,
                    'result': va.result
                })

        if verification_assignment.state != VerificationAssignment.State.FINISHED:
            data['verifiers'][verifier_id].append({
                'asset_id': verification_assignment.verifier_id,
                'state': verification_assignment.state,
                'progress': verification_assignment.progress,
                'spent_tflops': verification_assignment.tflops,
                'result': None
            })

    return data


def print_task_declaration(task_declaration):
    data = get_progress_data(task_declaration)

    logger.info('\n\n\n\n\n')

    logger.info('-------------------------------------------------------------------------------------------')

    logger.info('Task: {}\nState: {}\tProgress: {}\tTFLOPS: {}\tESTIMATED TFLOPS: {}'.format(
        data['asset_id'], magenta(data['state']), blue(data['total_progress']), cyan(data['spent_tflops']),
        cyan(data['estimated_tflops'])))

    logger.info('Dataset: {}'.format(data['dataset']))
    logger.info('Model: {}'.format(data['train_model']))
    logger.info('Workers: {}, Verifiers: {}'.format(
        yellow(data['accepted_workers']), yellow(data['accepted_verifiers'])))

    logger.info('Epochs: {}'.format(yellow('{}/{}'.format(data['current_epoch'], data['epochs']))))
    for epoch, value in data['history'].items():
        logger.info('Epoch #{}\tloss: {}\taccuracy: {}'.format(epoch, green(value['loss']), green(value['accuracy'])))

    logger.info('-------------------------------------------------------------------------------------------')

    for worker_id, worker_data in data['workers'].items():
        logger.info('\tWorker: {}'.format(worker_id))
        for wd in worker_data:
            logger.info('\t\tEpoch: #{}'.format(wd['current_epoch']))
            logger.info('\t\t\tState: {}\tProgress: {}\tTFLOPS: {}'.format(
                magenta(wd['state']), blue(wd['progress']), cyan(wd['spent_tflops'])))
            if wd['loss'] and wd['accuracy']:
                logger.info('\t\t\tloss: {}\taccuracy: {}'.format(green(wd['loss']), green(wd['accuracy'])))
            logger.info('\n')
    logger.info('-------------------------------------------------------------------------------------------')

    for verifier_id, verifier_data in data['verifiers'].items():
        logger.info('\tVerifier: {}'.format(verifier_id))
        epoch = 1
        for vd in verifier_data:
            logger.info('\t\tEpoch: #{}'.format(epoch))
            logger.info('\t\t\tState: {}\tProgress: {}\tTFLOPS: {}'.format(
                magenta(vd['state']), blue(vd['progress']), cyan(vd['spent_tflops'])))
            if vd['result']:
                result_text = ''
                for result in vd['result']:
                    result_text += '\n\t\t\t\tworker: {} - {}'.format(
                        result['worker_id'], green('FAKE' if result['is_fake'] else 'OK'))
                logger.info('\t\t\tResult: {}'.format(result_text))
            epoch += 1

    logger.info('-------------------------------------------------------------------------------------------')
    if task_declaration.state == TaskDeclaration.State.COMPLETED:
        logger.info('Result: {}'.format(yellow(task_declaration.weights)))


def main():
    parser = argparse.ArgumentParser(description='Monitor Task')

    parser.add_argument('-k', '--key', default='producer', metavar='KEY', help='producer RSA key name')
    parser.add_argument('-t', '--task', metavar='TASK_ASSET', help='asset id of task declaration')

    args = parser.parse_args()
    NodeContractInfo.init_poa(key_name='producer')

    Producer(
        account_address=NodeContractInfo.get_account_address(),
        rsa_pk_fs_name=args.key
    )

    task_declaration = TaskDeclaration.get(args.task)
    while task_declaration.state != TaskDeclaration.State.FAILED:
        print_task_declaration(task_declaration)

        time.sleep(3)
        task_declaration = TaskDeclaration.get(args.task)
        if task_declaration.state == TaskDeclaration.State.COMPLETED:
            print_task_declaration(task_declaration)
            break


if __name__ == '__main__':
    main()

