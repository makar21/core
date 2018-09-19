import sys
from logging import getLogger

from tatau_core.models import EstimationAssignment, EstimationResult
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
from tatau_core.utils.ipfs import Downloader
from .session import Session

logger = getLogger(__name__)


class EstimationSession(Session):
    def __init__(self, uuid=None):
        super(EstimationSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment: EstimationAssignment):
        assignment.estimation_result.state = EstimationResult.State.IN_PROGRESS
        assignment.estimation_result.save()

        downloader = Downloader(assignment.task_declaration_id)
        downloader.add_to_download_list(assignment.estimation_data.model_code, 'model.py')
        downloader.add_to_download_list(assignment.estimation_data.x_train, 'estimate_x_train')
        downloader.add_to_download_list(assignment.estimation_data.y_train, 'estimate_y_train')
        downloader.add_to_download_list(assignment.estimation_data.initial_weights, 'estimate_initial_weights')

        downloader.download_all()

        self.save_model_path(downloader.resolve_path('model.py'))
        self.save_x_train([downloader.resolve_path('estimate_x_train')])
        self.save_y_train([downloader.resolve_path('estimate_y_train')])
        self.save_init_weights_path(downloader.resolve_path('estimate_initial_weights'))

        assignment.estimation_result.progress = 20.0
        assignment.estimation_result.save()

        self._run(assignment.estimation_data.batch_size, 1, 1)

    def main(self):
        logger.info('Start estimation')
        batch_size = int(sys.argv[2])
        nb_epochs = int(sys.argv[3])
        current_iteration = int(sys.argv[4])

        model = Model.load_model(path=self.load_model_path())
        model.load_weights(self.load_init_weights_path())

        progress = TrainProgress()

        model.train(
            x_path_list=self.load_x_train(), y_path_list=self.load_y_train(),
            batch_size=batch_size, nb_epochs=nb_epochs, current_iteration=current_iteration,
            train_progress=progress
        )


if __name__ == '__main__':
    session = EstimationSession.run()
