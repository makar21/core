from collections import deque
import os
from .session import Session
from logging import getLogger
import sys
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
from tatau_core.tatau.models import TaskAssignment
from tatau_core.utils.ipfs import IPFS
import numpy as np
import pickle

logger = getLogger(__name__)


class TrainSession(Session):
    def __init__(self, uuid=None):
        super(TrainSession, self).__init__(module=__name__, uuid=uuid)

    # TODO: refactor to iterable
    @classmethod
    def concat_dataset(cls, x_paths, y_paths):
        x_train = None
        for train_x_path in x_paths:
            f = np.load(train_x_path)
            if x_train is not None:
                x_train = np.concatenate((x_train, f))
            else:
                x_train = f

        y_train = None
        for train_y_path in y_paths:
            f = np.load(train_y_path)
            if y_train is not None:
                y_train = np.concatenate((y_train, f))
            else:
                y_train = f

        return x_train, y_train

    @property
    def train_history_path(self):
        return os.path.join(self.base_dir, "train_history.pkl")

    @property
    def train_weights_path(self):
        return os.path.join(self.base_dir, "train_weights.pkl")

    def save_train_history(self, train_history):
        self.save_object(path=self.train_history_path, obj=train_history)

    def load_train_history(self):
        return self.load_object(self.train_history_path)

    def process_assignment(self, assignment: TaskAssignment):
        logger.info("Train Task: {}".format(assignment))

        ipfs = IPFS()

        logger.info('Train data: {}'.format(assignment.train_data))

        ipfs.download_to(assignment.train_data['model_code'], self.model_path)
        logger.info('model code successfully downloaded')

        batch_size = assignment.train_data['batch_size']
        epochs = assignment.train_data['epochs']

        train_x_paths = deque()
        for x_train in assignment.train_data['x_train_ipfs']:
            train_x_paths.append(ipfs.download(x_train, self.base_dir))

        logger.info('x_train is downloaded')

        train_y_paths = deque()
        for y_train in assignment.train_data['y_train_ipfs']:
            train_y_paths.append(ipfs.download(y_train, self.base_dir))

        logger.info('y_train is downloaded')

        # test_x_path = ipfs.download(assignment.train_data['x_test_ipfs'], target_dir)
        # logger.info('x_test is downloaded')
        #
        # test_y_path = ipfs.download(assignment.train_data['y_test_ipfs'], target_dir)
        # logger.info('y_test is downloaded')

        ipfs.download_to(assignment.train_data['initial_weights'], self.init_weights_path)
        logger.info('initial weights are downloaded')
        x_train, y_train = self.concat_dataset(x_paths=train_x_paths, y_paths=train_y_paths)

        np.save(self.x_train_path, x_train)
        np.save(self.y_train_path, y_train)
        logger.info('Dataset is loaded')

        model = Model.load_model(path=self.model_path)
        logger.info('Model is loaded')

        model.load_weights(self.init_weights_path)

        logger.info('Initial weights are loaded')

        logger.info('Start training')

        self._run(batch_size, epochs)

        assignment.train_history = self.load_train_history()

        assignment.loss = assignment.train_history['loss'][-1]
        assignment.accuracy = assignment.train_history['acc'][-1]

        ipfs_file = ipfs.add_file(self.train_weights_path)
        logger.info('Result weights are uploaded')

        assignment.result = ipfs_file.multihash

    def main(self):
        logger.info("Start training")
        batch_size = int(sys.argv[2])
        nb_epochs = int(sys.argv[3])

        model = Model.load_model(path=self.model_path)
        model.load_weights(self.init_weights_path)

        progress = TrainProgress()
        train_history = model.train(
            x=np.load(self.x_train_path), y=np.load(self.y_train_path),
            batch_size=batch_size, nb_epochs=nb_epochs,
            train_progress=progress
        )
        model.save_weights(self.train_weights_path)
        self.save_train_history(train_history)


if __name__ == '__main__':
    TrainSession.run()
