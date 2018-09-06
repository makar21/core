import sys
from logging import getLogger

import numpy as np

from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
from tatau_core.models import EstimationAssignment, EstimationResult
from tatau_core.utils.ipfs import Downloader
from .session import Session

logger = getLogger(__name__)


class EstimationSession(Session):
    def __init__(self, uuid=None):
        super(EstimationSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment: EstimationAssignment):
        assignment.estimation_result.state = EstimationResult.State.IN_PROGRESS
        assignment.estimation_result.save()

        list_download_params = [
            Downloader.DownloadParams(
                multihash=assignment.estimation_data.model_code,
                target_path=self.model_path
            ),
            Downloader.DownloadParams(
                multihash=assignment.estimation_data.x_train,
                target_path=self.x_train_path
            ),
            Downloader.DownloadParams(
                multihash=assignment.estimation_data.y_train,
                target_path=self.y_train_path
            ),
            Downloader.DownloadParams(
                multihash=assignment.estimation_data.initial_weights,
                target_path=self.init_weights_path
            )
        ]

        Downloader.download_all(list_download_params)
        assignment.estimation_result.progress = 20.0
        assignment.estimation_result.save()

        self._run(assignment.estimation_data.batch_size, 1)

    def main(self):
        logger.info("Start estimation")
        batch_size = int(sys.argv[2])
        nb_epochs = int(sys.argv[3])

        model = Model.load_model(path=self.model_path)
        model.load_weights(self.init_weights_path)

        progress = TrainProgress()

        model.train(
            x=np.load(self.x_train_path), y=np.load(self.y_train_path),
            batch_size=batch_size, nb_epochs=nb_epochs,
            train_progress=progress
        )


if __name__ == '__main__':
    session = EstimationSession.run()
