import os
import sys
from collections import deque
from logging import getLogger

from tatau_core.models import TaskAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
from tatau_core.utils import configure_logging
from tatau_core.utils.ipfs import IPFS, Downloader
from .session import Session, SessionValue

configure_logging()
logger = getLogger('tatau_core.trainer')


class TrainSession(Session):
    train_history = SessionValue()
    init_weights_path = SessionValue()
    chunk_dirs = SessionValue()
    train_weights_path = SessionValue()

    def __init__(self, uuid=None):
        super(TrainSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment: TaskAssignment, *args, **kwargs):
        logger.info('Train Task: {}'.format(assignment))

        train_result = assignment.train_result
        assert train_result

        logger.info('Train data: {}'.format(assignment.train_data))

        downloader = Downloader(assignment.task_declaration_id)
        downloader.add_to_download_list(assignment.train_data.model_code_ipfs, 'model.py')

        initial_weight_file_name = None
        if assignment.train_data.weights_ipfs is not None:
            initial_weight_file_name = 'initial_weight_{}'.format(assignment.train_data.current_iteration)
            downloader.add_to_download_list(assignment.train_data.weights_ipfs, initial_weight_file_name)
        else:
            logger.info('Initial weights are not set')

        batch_size = assignment.train_data.batch_size
        epochs = assignment.train_data.epochs

        chunk_dirs = deque()
        for index, chunk_ipfs in enumerate(assignment.train_data.train_chunks_ipfs):
            dir_name = 'chunk_{}'.format(index)
            downloader.add_to_download_list(chunk_ipfs, dir_name)
            chunk_dirs.append(downloader.resolve_path(dir_name))

        downloader.download_all()
        logger.info('Dataset downloaded')

        self.model_path = downloader.resolve_path('model.py')
        self.init_weights_path = None if initial_weight_file_name is None \
            else downloader.resolve_path(initial_weight_file_name)

        self.chunk_dirs = chunk_dirs

        logger.info('Start training')

        self._run(batch_size, epochs, assignment.train_data.current_iteration)

        train_result.train_history = self.train_history
        train_result.loss = train_result.train_history['loss'][-1]
        train_result.accuracy = train_result.train_history['acc'][-1]

        ipfs = IPFS()
        ipfs_file = ipfs.add_file(self.train_weights_path)
        logger.info('Result weights_ipfs are uploaded')

        train_result.weights_ipfs = ipfs_file.multihash

    def main(self):
        logger.info('Start training')
        batch_size = int(sys.argv[2])
        nb_epochs = int(sys.argv[3])
        current_iteration = int(sys.argv[4])

        model = Model.load_model(path=self.model_path)
        init_weights_path = self.init_weights_path
        if init_weights_path is not None:
            model.load_weights(init_weights_path)
        else:
            logger.info('Initial weights are not set')

        progress = TrainProgress()
        train_history = model.train(
            chunk_dirs=self.chunk_dirs,
            batch_size=batch_size, nb_epochs=nb_epochs,
            train_progress=progress, current_iteration=current_iteration
        )

        train_weights_path = os.path.join(self.base_dir, 'result_weights')
        model.save_weights(train_weights_path)
        self.train_weights_path = train_weights_path
        self.train_history = train_history


if __name__ == '__main__':
    logger.info("Start trainer")
    TrainSession.run()
