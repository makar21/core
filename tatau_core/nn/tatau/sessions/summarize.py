import os
from collections import deque, Iterable
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

    def save_results_list(self, result_list: Iterable):
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

        downloader = Downloader(assignment.task_declaration_id)
        downloader.add_to_download_list(assignment.verification_data.model_code_ipfs, 'model.py')

        downloaded_results = deque()
        for worker_result in assignment.verification_data.train_results:
            file_name = 'tr_{}_{}_{}'.format(worker_result['worker_id'], assignment.verification_data.current_iteration,
                                             assignment.verification_data.current_iteration_retry)

            downloader.add_to_download_list(worker_result['result'], file_name)
            downloaded_results.append(downloader.resolve_path(file_name))

        if not len(downloaded_results):
            logger.error('list of weights is empty')
            raise ValueError('list of weights is empty')

        downloader.download_all()

        self.save_model_path(downloader.resolve_path('model.py'))
        self.save_results_list(downloaded_results)

        self._run()

        ipfs = IPFS()
        verification_result.weights = ipfs.add_file(self.summarized_weights_path).multihash

    def main(self):
        logger.info('Run Summarizer')
        results_list = self.load_results_list()
        model = Model.load_model(self.load_model_path())

        summarizer = model.get_weights_summarizer()
        serializer = model.get_weights_serializer()

        for weights_path in results_list:
            weights = serializer.load(weights_path)
            summarizer.update(weights=weights)

        weights = summarizer.commit()
        serializer.save(path=self.summarized_weights_path, weights=weights)


if __name__ == '__main__':
    session = SummarizeSession.run()
