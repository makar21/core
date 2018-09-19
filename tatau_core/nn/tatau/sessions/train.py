import os
import sys
from collections import deque
from logging import getLogger

from tatau_core.models import TaskAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
from tatau_core.utils.ipfs import IPFS, Downloader
from .session import Session

logger = getLogger(__name__)


class TrainSession(Session):
    def __init__(self, uuid=None):
        super(TrainSession, self).__init__(module=__name__, uuid=uuid)

    @property
    def train_history_path(self):
        return os.path.join(self.base_dir, 'train_history.pkl')

    @property
    def train_weights_path(self):
        return os.path.join(self.base_dir, 'train_weights.pkl')

    def save_train_history(self, train_history):
        self.save_object(path=self.train_history_path, obj=train_history)

    def load_train_history(self):
        return self.load_object(self.train_history_path)

    def process_assignment(self, assignment: TaskAssignment, *args, **kwargs):
        logger.info('Train Task: {}'.format(assignment))

        train_result = assignment.train_result
        assert train_result

        logger.info('Train data: {}'.format(assignment.train_data))

        downloader = Downloader(assignment.task_declaration_id)
        downloader.add_to_download_list(assignment.train_data.model_code, 'model.py')

        initial_weight_file_name = 'initial_weight_{}'.format(assignment.train_data.current_iteration)
        downloader.add_to_download_list(assignment.train_data.initial_weights, initial_weight_file_name)

        batch_size = assignment.train_data.batch_size
        epochs = assignment.train_data.epochs

        train_x_paths = deque()
        for index, x_train in enumerate(assignment.train_data.x_train):
            file_name = 'x_{}'.format(index)
            downloader.add_to_download_list(x_train, file_name)
            train_x_paths.append(downloader.resolve_path(file_name))

        train_y_paths = deque()
        for index, y_train in enumerate(assignment.train_data.y_train):
            file_name = 'y_{}'.format(index)
            downloader.add_to_download_list(y_train, file_name)
            train_y_paths.append(downloader.resolve_path(file_name))

        downloader.download_all()

        self.save_model_path(downloader.resolve_path('model.py'))
        self.save_init_weights_path(downloader.resolve_path(initial_weight_file_name))
        self.save_x_train(train_x_paths)
        self.save_y_train(train_y_paths)

        logger.info('Dataset downloaded')

        logger.info('Start training')

        self._run(batch_size, epochs, assignment.train_data.current_iteration)

        train_result.train_history = self.load_train_history()

        train_result.loss = train_result.train_history['loss'][-1]
        train_result.accuracy = train_result.train_history['acc'][-1]

        ipfs = IPFS()
        ipfs_file = ipfs.add_file(self.train_weights_path)
        logger.info('Result weights are uploaded')

        train_result.weights = ipfs_file.multihash

    def main(self):
        logger.info('Start training')
        batch_size = int(sys.argv[2])
        nb_epochs = int(sys.argv[3])
        current_iteration = int(sys.argv[4])

        model = Model.load_model(path=self.load_model_path())
        model.load_weights(self.load_init_weights_path())

        progress = TrainProgress()
        train_history = model.train(
            x_path_list=self.load_x_train(), y_path_list=self.load_y_train(),
            batch_size=batch_size, nb_epochs=nb_epochs,
            train_progress=progress, current_iteration=current_iteration
        )
        model.save_weights(self.train_weights_path)
        self.save_train_history(train_history)


if __name__ == '__main__':
    TrainSession.run()
