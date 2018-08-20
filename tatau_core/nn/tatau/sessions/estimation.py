import sys
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.progress import TrainProgress
import numpy as np
from logging import getLogger

from tatau_core.tatau.models import EstimationAssignment
from .session import Session

from tatau_core.utils.ipfs import IPFS

logger = getLogger(__name__)


class EstimationSession(Session):
    def __init__(self, uuid=None):
        super(EstimationSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment: EstimationAssignment):
        ipfs = IPFS()

        ipfs.download_to(assignment.estimation_data['model_code'], self.model_path)
        logger.info('model code successfully downloaded')

        ipfs.download_to(assignment.estimation_data['x_train'], self.x_train_path)
        logger.info('x_train is downloaded')

        ipfs.download_to(assignment.estimation_data['y_train'], self.y_train_path)
        logger.info('x_train is downloaded')

        ipfs.download_to(assignment.estimation_data['initial_weights'], self.init_weights_path)
        logger.info('initial weights are downloaded')

        self._run(assignment.estimation_data['batch_size'], 1)

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
