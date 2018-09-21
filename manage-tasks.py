import argparse
import os
import shutil
import tempfile
import time
from logging import getLogger

from termcolor import colored
import glob
from producer import load_producer
from tatau_core import settings
from tatau_core.contract import NodeContractInfo, poa_wrapper
from tatau_core.models import TaskDeclaration, TrainModel, Dataset
from tatau_core.nn.tatau.model import Model, TrainProgress
from tatau_core.utils.ipfs import IPFS
from tatau_core.utils.logging import configure_logging
from glob import glob

configure_logging(__name__)

logger = getLogger(__name__)


def train_local(train_dir, test_dir, model_path, batch_size, epochs):
    model = Model.load_model(path=model_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    train_chunks = [os.path.join(train_dir, chunk_dir) for chunk_dir in os.listdir(train_dir)]
    test_chunks = [os.path.join(test_dir, chunk_dir) for chunk_dir in os.listdir(test_dir)]

    model.train(
        chunk_dirs=train_chunks,
        batch_size=batch_size,
        current_iteration=1,
        nb_epochs=epochs,
        train_progress=LocalProgress())

    loss, acc = model.eval(chunk_dirs=test_chunks)

    print('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def train_remote(train_ipfs, test_ipfs, args):
    logger.info('Start remote train')

    producer = load_producer()

    logger.info('Generate initial model weights_ipfs')
    model = Model.load_model(path=args.path)

    initial_weights_path = '/tmp/tatau_initial_weights'
    model.save_weights(initial_weights_path)

    ipfs = IPFS()
    logger.info('Upload weights_ipfs to IPFS')
    initial_weights_file = ipfs.add_file(initial_weights_path)

    os.unlink(initial_weights_path)

    dataset_name = os.path.basename(args.name)

    dataset = Dataset.create(
        db=producer.db,
        encryption=producer.encryption,
        name=dataset_name,
        train_dir_ipfs=train_ipfs,
        test_dir_ipfs=test_ipfs
    )

    # logger.info('Dataset created: {}'.format(dataset))
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
        weights_ipfs=initial_weights_file.multihash,
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


def print_task_declaration(task_declaration):
    data = task_declaration.progress_info

    logger.info('\n\n\n\n\n')

    logger.info('-------------------------------------------------------------------------------------------')

    logger.info('Task: {}\nState: {}\tProgress: {}\tTFLOPS: {}\tESTIMATED TFLOPS: {}'.format(
        data['asset_id'], magenta(data['state']), blue(data['total_progress']), cyan(data['spent_tflops']),
        cyan(data['estimated_tflops'])))

    logger.info('Dataset: {}'.format(data['dataset']))
    logger.info('Model: {}'.format(data['train_model']))
    logger.info('Estimators:{} Workers: {}, Verifiers: {}'.format(
        yellow((data['accepted_estimators'])), yellow(data['accepted_workers']), yellow(data['accepted_verifiers'])))

    logger.info('Epochs: {}'.format(yellow('{}/{}'.format(data['current_epoch'], data['epochs']))))

    logger.info('Current iteration: {}'.format(yellow(data['current_iteration'])))

    for iteration, value in data['history'].items():
        logger.info('Iteration #{}\tloss: {}\taccuracy: {}\tduration: {}'.format(
            iteration, green(value['loss']), green(value['accuracy']), green(value['duration'])))

    logger.info('-------------------------------------------------------------------------------------------')

    logger.info('\tTrain')
    for iteration, worker_data in data['workers'].items():
        logger.info('\tIteration: {}'.format(iteration))
        for worker_info in worker_data:
            logger.info('\tWorker: {}'.format(worker_info['worker_id']))
            logger.info('\t\t\tState: {}\tProgress: {}\tTFLOPS: {}'.format(
                magenta(worker_info['state']), blue(worker_info['progress']), cyan(worker_info['spent_tflops'])))
            if worker_info['loss'] and worker_info['accuracy']:
                logger.info('\t\t\tloss: {}\taccuracy: {}'.format(
                    green(worker_info['loss']), green(worker_info['accuracy'])))
            logger.info('\n')
    logger.info('-------------------------------------------------------------------------------------------')

    logger.info('\tVerification')
    for iteration, verifier_data in data['verifiers'].items():
        logger.info('\t\tIteration: #{}'.format(iteration))
        for verifier_info in verifier_data:
            logger.info('\t\t\tState: {}\tProgress: {}\tTFLOPS: {}'.format(
                magenta(verifier_info['state']), blue(verifier_info['progress']), cyan(verifier_info['spent_tflops'])))
            if verifier_info['results']:
                result_text = ''
                for result in verifier_info['results']:
                    result_text += '\n\t\t\t\tworker: {} - {}'.format(
                        result['worker_id'], green('FAKE' if result['is_fake'] else 'OK'))
                logger.info('\t\t\tResult: {}'.format(result_text))
            iteration += 1

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
    parser.add_argument('-p', '--path', default='examples/torch/mnist/cnn.py', metavar='PATH', help='model path')
    parser.add_argument('-train', '--dataset_train', default='QmR8scAnnzQRvPV23a6MgTTVWQQ3yhxc6mSXksKMx6YTRy', metavar='dataset', help='dataset dir')
    parser.add_argument('-test', '--dataset_test', default='QmWJyj6zYpV9vFGKUb65tz66j884fgAqPdYiKmRr97NYKe',metavar='dataset', help='dataset dir')
    parser.add_argument('-train', '--dataset_train', default='QmR8scAnnzQRvPV23a6MgTTVWQQ3yhxc6mSXksKMx6YTRy', metavar='dataset_train', help='dataset dir')
    parser.add_argument('-test', '--dataset_test', default='QmXJD9uVTLpvTeLPCgRzZHscZQWyG8LeWQ1Hecw3dfjNzn', metavar='dataset_test', help='dataset dir')
    parser.add_argument('-w', '--workers', default=1, type=int, metavar='WORKERS', help='workers count')
    parser.add_argument('-v', '--verifiers', default=1, type=int, metavar='VERIFIERS', help='verifiers count')
    parser.add_argument('-b', '--batch', default=128, type=int, metavar='BATCH_SIZE', help='batch size')
    parser.add_argument('-e', '--epochs', default=3, type=int, metavar='EPOCHS', help='epochs')
    parser.add_argument('-ei', '--epochs_in_iteration', default=1, type=int, metavar='EPOCHS IN ITERATION', help='epochs in iteration')
    parser.add_argument('-l', '--local', default=1, type=int, metavar='LOCAL', help='train model local')
    parser.add_argument('-t', '--task', default=None, type=str, metavar='TASK_ID', help='task declaration asset id')
    parser.add_argument('-eth', '--eth', default=None, type=float, metavar='ETH', help='ETH for deposit or issue')

    args = parser.parse_args()

    if args.command == 'add':
        if args.local:
            train_local(
                train_dir=args.dataset_train, test_dir=args.dataset_test,
                model_path=args.path, batch_size=args.batch, epochs=args.epochs
            )
        else:
            train_remote(
                train_ipfs=args.dataset_train, test_ipfs=args.dataset_test,
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
