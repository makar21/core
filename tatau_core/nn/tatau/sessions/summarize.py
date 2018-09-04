import os
from collections import deque
from logging import getLogger

import numpy as np

from tatau_core.nn.tatau.model import Model
from tatau_core.models import VerificationAssignment
from tatau_core.utils.ipfs import IPFS, Downloader
from .session import Session

logger = getLogger(__name__)


class SummarizeSession(Session):
    def __init__(self, uuid=None):
        super(SummarizeSession, self).__init__(module=__name__, uuid=uuid)

    @property
    def x_test_path(self):
        return os.path.join(self.base_dir, "x_test.npy")

    @property
    def y_test_path(self):
        return os.path.join(self.base_dir, "y_test.npy")

    @property
    def results_list_path(self):
        return os.path.join(self.base_dir, "results_list.pkl")

    def save_results_list(self, result_list: list):
        self.save_object(self.results_list_path, result_list)

    def load_results_list(self):
        return self.load_object(self.results_list_path)

    @property
    def summarized_weights_path(self):
        return os.path.join(self.base_dir, "summarized_weights.pkl")

    @property
    def eval_result_path(self):
        return os.path.join(self.base_dir, "eval.pkl")

    def save_eval_result(self, loss, acc):
        self.save_object(path=self.eval_result_path, obj=dict(loss=loss, acc=acc))

    def load_eval_result(self):
        result = self.load_object(path=self.eval_result_path)
        return result['loss'], result['acc']

    def process_assignment(self, assignment: VerificationAssignment):
        verification_assignment = assignment

        list_download_params = [
            Downloader.DownloadParams(
                multihash=verification_assignment.x_test_ipfs,
                target_path=self.x_test_path
            ),
            Downloader.DownloadParams(
                multihash=verification_assignment.y_test_ipfs,
                target_path=self.y_test_path
            ),
            Downloader.DownloadParams(
                multihash=verification_assignment.model_code_ipfs,
                target_path=self.model_path
            ),
        ]

        downloaded_results = deque()
        for worker_result in verification_assignment.train_results:
            target_path = os.path.join(self.base_dir, worker_result['result'])
            list_download_params.append(Downloader.DownloadParams(multihash=worker_result['result'], target_path=target_path))
            downloaded_results.append(target_path)

        if not len(downloaded_results):
            logger.error('list of weights is empty')
            raise ValueError('list of weights is empty')

        Downloader.download_all(list_download_params)
        self.save_results_list(list(downloaded_results))

        self._run()

        ipfs = IPFS()
        verification_assignment.loss, verification_assignment.accuracy = self.load_eval_result()
        verification_assignment.weights = ipfs.add_file(self.summarized_weights_path).multihash

    def main(self):
        logger.info("Run Summarizer")
        results_list = self.load_results_list()
        model = Model.load_model(self.model_path)

        summarizer = model.get_weights_summarizer()
        serializer = model.get_weights_serializer()

        for weights_path in results_list:
            weights = serializer.load(weights_path)
            summarizer.update(weights=weights)

        weights = summarizer.commit()

        model.set_weights(weights)

        loss, acc = model.eval(x=np.load(self.x_test_path), y=np.load(self.y_test_path))
        self.save_eval_result(loss=loss, acc=acc)
        model.save_weights(self.summarized_weights_path)


if __name__ == '__main__':
    session = SummarizeSession.run()
