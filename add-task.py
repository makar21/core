from logging import getLogger
import argparse
from tatau_core.nn.models.tatau import TatauModel, TrainProgress
from tatau_core.tatau.models import TrainModel, Dataset, TaskDeclaration
from tatau_core.tatau.node.producer import Producer
from tatau_core.utils.ipfs import IPFS
from tatau_core.utils.logging import configure_logging
import os
import numpy as np

configure_logging(__name__)

logger = getLogger(__name__)


def train_local(x_train_path, y_train_path, x_test_path, y_test_path, model_path, batch_size, epochs):
    model = TatauModel.load_model(path=model_path)

    x_train = np.load(x_train_path)
    y_train = np.load(y_train_path)
    x_test = np.load(x_test_path)
    y_test = np.load(y_test_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    history = model.train(x=x_train, y=y_train, batch_size=batch_size, nb_epochs=epochs, train_progress=LocalProgress())

    train_history = dict()
    for metric in history.history.keys():
        train_history[metric] = [float(val) for val in history.history[metric]]

    for lr in history.history['lr']:
        print('lr({}):{}'.format(lr.__class__.__name__, lr))

    for loss in history.history['loss']:
        print('loss({}):{}'.format(loss.__class__.__name__, loss))

    for acc in history.history['acc']:
        print('acc({}):{}'.format(acc.__class__.__name__, acc))

    loss, acc = model.eval(x=x_test, y=y_test)

    print('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def train_remote(x_train_path, y_train_path, x_test_path, y_test_path, args):
    logger.info("Start remote train")
    producer = Producer(rsa_pk_fs_name=args.key)

    logger.info("Generate initial model weights")
    model = TatauModel.load_model(path=args.path)

    initial_weights = model.get_weights()

    initial_weights_path = "/tmp/tatau_initial_weights.npz"
    np.savez(initial_weights_path, *initial_weights)
    # weights_file = np.load(initial_weights_path)
    # model.set_weights([weights_file[r] for r in weights_file])

    ipfs = IPFS()
    logger.info("Upload weights to IPFS")
    initial_weights_file = ipfs.add_file(initial_weights_path)

    os.unlink(initial_weights_path)

    dataset = Dataset.upload_and_create(
        db=producer.db,
        encryption=producer.encryption,
        name=args.name,
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
        code_path=args.path
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
        weights=initial_weights_file.multihash
    )

    logger.debug('Train job created: {}'.format(task))


def main():
    parser = argparse.ArgumentParser(description='Produce Task')

    parser.add_argument('-k', '--key', default="producer", metavar='KEY', help='RSA key name')
    parser.add_argument('-n', '--name', default='mnist_mlp', metavar='NAME', help='model name')
    parser.add_argument('-p', '--path', default='examples/keras/mnist/mlp.py', metavar='PATH', help='model path')
    parser.add_argument('-d', '--dataset', default='examples/keras/mnist', metavar='dataset', help='dataset dir')
    parser.add_argument('-w', '--workers', default=1, type=int, metavar='WORKERS', help='workers count')
    parser.add_argument('-v', '--verifiers', default=1, type=int, metavar='VERIFIERS', help='verifiers count')
    parser.add_argument('-b', '--batch', default=128, type=int, metavar='BATCH_SIZE', help='batch size')
    parser.add_argument('-e', '--epochs', default=3, type=int, metavar='EPOCHS', help='epochs')
    parser.add_argument('-l', '--local', default=1, type=int, metavar='LOCAL', help='train model local')
    args = parser.parse_args()
    dataset_name = os.path.basename(args.dataset)

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


if __name__ == '__main__':
    main()
