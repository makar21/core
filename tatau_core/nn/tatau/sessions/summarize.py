import os
from collections import deque
from glob import glob
from logging import getLogger

from tatau_core.models import VerificationAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.utils.ipfs import IPFS, Downloader
from .session import Session

logger = getLogger(__name__)


class SummarizeSession(Session):
    def __init__(self, uuid=None):
        super(SummarizeSession, self).__init__(module=__name__, uuid=uuid)

    @property
    def results_list_path(self):
        return os.path.join(self.base_dir, 'results_list.pkl')

    def save_results_list(self, result_list: list):
        self.save_object(self.results_list_path, result_list)

    def load_results_list(self):
        return self.load_object(self.results_list_path)

    @property
    def summarized_weights_path(self):
        return os.path.join(self.base_dir, 'summarized_weights.pkl')

    @property
    def eval_result_path(self):
        return os.path.join(self.base_dir, 'eval.pkl')

    def save_eval_result(self, loss, acc):
        self.save_object(path=self.eval_result_path, obj=dict(loss=loss, acc=acc))

    def load_eval_result(self):
        result = self.load_object(path=self.eval_result_path)
        return result['loss'], result['acc']

    def process_assignment(self, assignment: VerificationAssignment, *args, **kwargs):
        verification_assignment = assignment
        verification_result = verification_assignment.verification_result

        list_download_params = [
            Downloader.DownloadParams(
                multihash=verification_assignment.verification_data.test_dir_ipfs,
                target_path=self.base_dir,
                directory=True
            ),
            Downloader.DownloadParams(
                multihash=verification_assignment.verification_data.model_code_ipfs,
                target_path=self.model_path
            ),
        ]

        downloaded_results = deque()
        for worker_result in verification_assignment.verification_data.train_results:
            target_path = os.path.join(self.base_dir, worker_result['result'])
            list_download_params.append(Downloader.DownloadParams(
                multihash=worker_result['result'], target_path=target_path))
            downloaded_results.append(target_path)

        if not len(downloaded_results):
            logger.error('list of weights is empty')
            raise ValueError('list of weights is empty')

        Downloader.download_all(list_download_params)
        self.save_results_list(list(downloaded_results))

        test_dir = os.path.join(self.base_dir, verification_assignment.verification_data.test_dir_ipfs)
        x_test_paths = sorted(glob(os.path.join(test_dir, 'x_test*')))
        y_test_paths = sorted(glob(os.path.join(test_dir, 'y_test*')))

        self.save_x_test(x_test_paths)
        self.save_y_test(y_test_paths)

        self._run()

        ipfs = IPFS()

        loss, accuracy = self.load_eval_result()
        verification_result.loss = loss
        verification_result.accuracy = accuracy
        verification_result.weights = ipfs.add_file(self.summarized_weights_path).multihash

    def main(self):
        logger.info('Run Summarizer')
        results_list = self.load_results_list()
        model = Model.load_model(self.model_path)

        summarizer = model.get_weights_summarizer()
        serializer = model.get_weights_serializer()

        for weights_path in results_list:
            weights = serializer.load(weights_path)
            summarizer.update(weights=weights)

        weights = summarizer.commit()

        model.set_weights(weights)

        loss, acc = model.eval(x_path_list=self.load_x_test(), y_path_list=self.load_y_test())
        self.save_eval_result(loss=loss, acc=acc)
        model.save_weights(self.summarized_weights_path)


if __name__ == '__main__':
    session = SummarizeSession.run()
