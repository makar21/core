import argparse
import os
import time
from logging import getLogger

import numpy as np
from termcolor import colored

from producer import load_producer
from tatau_core import settings
from tatau_core.contract import NodeContractInfo, poa_wrapper
from tatau_core.nn.tatau.model import Model, TrainProgress
from tatau_core.tatau.models import TaskDeclaration, TaskAssignment, VerificationAssignment
from tatau_core.tatau.models import TrainModel, Dataset
from tatau_core.utils.ipfs import IPFS
from tatau_core.utils.logging import configure_logging

configure_logging(__name__)

logger = getLogger(__name__)


def train_local(x_train_path, y_train_path, x_test_path, y_test_path, model_path, batch_size, epochs):
    model = Model.load_model(path=model_path)

    x_train = np.load(x_train_path)
    y_train = np.load(y_train_path)
    x_test = np.load(x_test_path)
    y_test = np.load(y_test_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    history = model.train(x=x_train, y=y_train, batch_size=batch_size, nb_epochs=epochs, train_progress=LocalProgress())

    print(history)
    loss, acc = model.eval(x=x_test, y=y_test)
    print('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def train_remote(x_train_path, y_train_path, x_test_path, y_test_path, args):
    logger.info("Start remote train")

    producer = load_producer()

    logger.info("Generate initial model weights")
    model = Model.load_model(path=args.path)

    initial_weights_path = "/tmp/tatau_initial_weights"
    model.save_weights(initial_weights_path)

    ipfs = IPFS()
    logger.info("Upload weights to IPFS")
    initial_weights_file = ipfs.add_file(initial_weights_path)

    os.unlink(initial_weights_path)

    dataset_name = os.path.basename(args.dataset)

    dataset = Dataset.upload_and_create(
        db=producer.db,
        encryption=producer.encryption,
        name=dataset_name,
        x_train_path=x_train_path,
        y_train_path=y_train_path,
        x_test_path=x_test_path,
        y_test_path=y_test_path,
        minibatch_size=1000
    )

    logger.info('Dataset created: {}'.format(dataset))
    logger.info('Create model')
    train_model = TrainModel.upload_and_create(
        name=args.name,
        code_path=args.path,
        db=producer.db,
        encryption=producer.encryption
    )

    logger.debug('Model created: {}'.format(train_model))

    logger.info('Create train job')
    task = TaskDeclaration.create(
        producer_id=producer.asset_id,
        dataset_id=dataset.asset_id,
        train_model_id=train_model.asset_id,
        workers_needed=args.workers,
        verifiers_needed=args.verifiers,
        batch_size=args.batch,
        epochs=args.epochs,
        weights=initial_weights_file.multihash,
        db=producer.db,
        encryption=producer.encryption,
        epochs_in_iteration=args.epochs_in_iteration
    )

    logger.debug('Train job created: {}'.format(task))


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
        'current_iteration': task_declaration.current_iteration,
        'epochs_in_iteration': task_declaration.epochs_in_iteration,
        'epochs': task_declaration.epochs,
        'history': {},
        'spent_tflops': task_declaration.tflops,
        'estimated_tflops': task_declaration.estimated_tflops,
        'workers': {},
        'verifiers': {}
    }

    if task_declaration.state == TaskDeclaration.State.COMPLETED:
        data['train_result'] = task_declaration.weights

    for td in TaskDeclaration.get_history(
            task_declaration.asset_id, db=task_declaration.db, encryption=task_declaration.encryption):
        if td.loss and td.accuracy and td.state in [TaskDeclaration.State.EPOCH_IN_PROGRESS,
                                                    TaskDeclaration.State.COMPLETED]:
            if td.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
                epoch = td.current_iteration - 1
            else:
                epoch = td.current_iteration

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
        history = TaskAssignment.get_history(
            task_assignment.asset_id, db=task_declaration.db, encryption=task_declaration.encryption)
        for ta in history:
            if ta.state == TaskAssignment.State.FINISHED:
                data['workers'][worker_id].append({
                    'asset_id': ta.worker_id,
                    'state': ta.state,
                    'current_iteration': ta.current_iteration,
                    'progress': ta.progress,
                    'spent_tflops': ta.tflops,
                    'loss': ta.loss,
                    'accuracy': ta.accuracy
                })

        if task_assignment.state != TaskAssignment.State.FINISHED:
            data['workers'][worker_id].append({
                'asset_id': task_assignment.asset_id,
                'state': task_assignment.state,
                'current_iteration': task_assignment.current_iteration,
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
        history = VerificationAssignment.get_history(
            verification_assignment.asset_id, db=task_declaration.db, encryption=task_declaration.encryption)
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

    logger.info('Epochs: {}'.format(yellow('{}/{}'.format(
        min(data['current_iteration'] * data['epochs_in_iteration'], data['epochs']), data['epochs']))))
    for epoch, value in data['history'].items():
        logger.info('Iteration #{}\tloss: {}\taccuracy: {}'.format(epoch, green(value['loss']), green(value['accuracy'])))

    logger.info('-------------------------------------------------------------------------------------------')

    for worker_id, worker_data in data['workers'].items():
        logger.info('\tWorker: {}'.format(worker_id))
        for wd in worker_data:
            logger.info('\t\tIteration: #{}'.format(wd['current_iteration']))
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
            logger.info('\t\tIteration: #{}'.format(epoch))
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


def monitor_task(asset_id, producer):
    task_declaration = TaskDeclaration.get(asset_id, db=producer.db, encryption=producer.encryption)
    logger.info('{} sate {}'.format(task_declaration, task_declaration.state))
    while task_declaration.state != TaskDeclaration.State.FAILED:
        print_task_declaration(task_declaration)

        time.sleep(3)
        task_declaration = TaskDeclaration.get(asset_id, db=producer.db, encryption=producer.encryption)
        if task_declaration.state == TaskDeclaration.State.COMPLETED:
            print_task_declaration(task_declaration)
            break


def load_wallet_credentials(account_address_var_name):
    address = os.getenv(account_address_var_name)
    if address is None:
        raise ValueError('{} is not specified'.format(account_address_var_name))

    storage_path = settings.KEYS_PATH
    dir_name = address.replace('0x', '')
    with open(os.path.join(storage_path, dir_name, 'wallet.json'), 'r') as f:
        wallet = f.read()

    with open(os.path.join(storage_path, dir_name, 'wallet.pass'), 'r') as f:
        wallet_password = f.read()

    return wallet, wallet_password


def main():
    parser = argparse.ArgumentParser(description='Produce Task')

    parser.add_argument('-c', '--command', required=True, metavar='KEY', help='add|stop|cancel|issue|deposit|monitor')
    parser.add_argument('-k', '--key', default="producer", metavar='KEY', help='RSA key name')
    parser.add_argument('-n', '--name', default='mnist_mlp', metavar='NAME', help='model name')
    parser.add_argument('-p', '--path', default='examples/keras/mnist/mlp.py', metavar='PATH', help='model path')
    parser.add_argument('-d', '--dataset', default='examples/keras/mnist', metavar='dataset', help='dataset dir')
    parser.add_argument('-w', '--workers', default=1, type=int, metavar='WORKERS', help='workers count')
    parser.add_argument('-v', '--verifiers', default=1, type=int, metavar='VERIFIERS', help='verifiers count')
    parser.add_argument('-b', '--batch', default=128, type=int, metavar='BATCH_SIZE', help='batch size')
    parser.add_argument('-e', '--epochs', default=3, type=int, metavar='EPOCHS', help='epochs')
    parser.add_argument('-ei', '--epochs_in_iteration', default=3, type=int, metavar='EPOCHS IN ITERATION',
                        help='epochs in iteration')
    parser.add_argument('-l', '--local', default=0, type=int, metavar='LOCAL', help='train model local')
    parser.add_argument('-t', '--task', default=None, type=str, metavar='TASK_ID', help='task declaration asset id')
    parser.add_argument('-eth', '--eth', default=None, type=float, metavar='ETH', help='ETH for deposit or issue')

    args = parser.parse_args()

    if args.command == 'add':
        x_train_path = os.path.join(args.dataset, 'x_train.npy')
        y_train_path = os.path.join(args.dataset, 'y_train.npy')
        x_test_path = os.path.join(args.dataset, 'x_test.npy')
        y_test_path = os.path.join(args.dataset, 'y_test.npy')

        if args.local:
            train_local(
                x_train_path=x_train_path, y_train_path=y_train_path, x_test_path=x_test_path, y_test_path=y_test_path,
                model_path=args.path, batch_size=args.batch, epochs=args.epochs
            )
        else:
            train_remote(
                x_train_path=x_train_path, y_train_path=y_train_path, x_test_path=x_test_path, y_test_path=y_test_path,
                args=args
            )
        return

    producer = load_producer()
    if not args.task:
        print('task is not specified, arg: -t')
        return

    if args.command == 'cancel':
        td = TaskDeclaration.get(args.task, db=producer.db, encryption=producer.encryption)
        td.state = TaskDeclaration.State.FAILED
        td.save()
        print('Canceled {}'.format(td))
        return

    if args.command == 'stop':
        td = TaskDeclaration.get(args.task, db=producer.db, encryption=producer.encryption)
        td.state = TaskDeclaration.State.COMPLETED
        td.save()
        print('Stopped {}'.format(td))
        return

    if args.command == 'monitor':
        monitor_task(args.task, producer)
        return

    if args.command == 'issue':
        if not args.eth:
            print('balance is not specified, arg: --eth')
            return

        encrypted_key, password = load_wallet_credentials(account_address_var_name='PRODUCER_ACCOUNT_ADDRESS')
        NodeContractInfo.configure(encrypted_key, password)
        task_declaration = TaskDeclaration.get(args.task, db=producer.db, encryption=producer.encryption)
        poa_wrapper.issue_job(task_declaration, args.eth)
        return

    if args.command == 'deposit':
        if not args.eth:
            print('balance is not specified, arg: --eth')
            return

        encrypted_key, password = load_wallet_credentials(account_address_var_name='PRODUCER_ACCOUNT_ADDRESS')
        NodeContractInfo.configure(encrypted_key, password)
        task_declaration = TaskDeclaration.get(args.task, db=producer.db, encryption=producer.encryption)
        poa_wrapper.deposit(task_declaration, args.eth)
        return


if __name__ == '__main__':
    main()
