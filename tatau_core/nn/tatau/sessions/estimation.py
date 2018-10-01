import sys
from logging import getLogger

from tatau_core.models import EstimationAssignment, EstimationResult
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
from tatau_core.utils.ipfs import Downloader
from .session import Session, SessionValue

logger = getLogger('tatau_core')


class EstimationSession(Session):
    train_chunk_dir = SessionValue()
    init_weights_path = SessionValue()

    def __init__(self, uuid=None):
        super(EstimationSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment: EstimationAssignment):
        assignment.estimation_result.state = EstimationResult.State.IN_PROGRESS
        assignment.estimation_result.save()

        downloader = Downloader(assignment.task_declaration_id)
        downloader.add_to_download_list(assignment.estimation_data.model_code_ipfs, 'model.py')
        downloader.add_to_download_list(assignment.estimation_data.chunk_ipfs, 'estimate_train')

        initial_weight_file_name = None
        if assignment.estimation_data.weights_ipfs is not None:
            initial_weight_file_name = 'estimate_initial_weights'
            downloader.add_to_download_list(assignment.estimation_data.weights_ipfs, initial_weight_file_name)
        else:
            logger.info('Initial weights are not set')

        downloader.download_all()

        self.model_path = downloader.resolve_path('model.py')
        self.train_chunk_dir = downloader.resolve_path('estimate_train')
        self.init_weights_path = None if initial_weight_file_name is None \
            else downloader.resolve_path('estimate_initial_weights')

        assignment.estimation_result.progress = 20.0
        assignment.estimation_result.save()

        self._run(assignment.estimation_data.batch_size, 1, 1)

    def main(self):
        logger.info('Start estimation')
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

        model.train(
            chunk_dirs=[self.train_chunk_dir],
            batch_size=batch_size, nb_epochs=nb_epochs, current_iteration=current_iteration,
            train_progress=progress
        )


if __name__ == '__main__':
    session = EstimationSession.run()
