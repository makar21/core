import os
from collections import deque
from logging import getLogger

from tatau_core.models import VerificationAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.utils.ipfs import IPFS, Downloader
from .session import Session, SessionValue

logger = getLogger('tatau_core')


class SummarizeSession(Session):
    results_list = SessionValue()
    summarized_weights_path = SessionValue()

    def __init__(self, uuid=None):
        super(SummarizeSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment: VerificationAssignment, *args, **kwargs):
        verification_assignment = assignment
        verification_result = verification_assignment.verification_result

        downloader = Downloader(assignment.task_declaration_id)
        downloader.add_to_download_list(assignment.verification_data.model_code_ipfs, 'model.py')

        downloaded_results = deque()
        ipfs_weights = deque()

        for worker_result in assignment.verification_data.train_results:
            file_name = 'tr_{}_{}_{}'.format(worker_result['worker_id'], assignment.verification_data.current_iteration,
                                             assignment.verification_data.current_iteration_retry)

            downloader.add_to_download_list(worker_result['result'], file_name)
            downloaded_results.append(downloader.resolve_path(file_name))

            ipfs_weights.append(worker_result['result'])

        if not len(downloaded_results):
            logger.error('list of weights_ipfs is empty')
            raise ValueError('list of weights_ipfs is empty')

        downloader.download_all()

        self.model_path = downloader.resolve_path('model.py')
        self.results_list = downloaded_results

        self._run()

        ipfs = IPFS()
        verification_result.weights_ipfs = ipfs.add_file(self.summarized_weights_path).multihash

        for multihash in ipfs_weights:
            downloader.remove_from_storage(multihash)

    def main(self):
        logger.info('Run Summarizer')
        results_list = self.results_list
        model = Model.load_model(self.model_path)

        summarizer = model.get_weights_summarizer()
        serializer = model.get_weights_serializer()

        for weights_path in results_list:
            weights = serializer.load(weights_path)
            summarizer.update(weights=weights)

        weights = summarizer.commit()
        summarized_weights_path = os.path.join(self.base_dir, 'summarized_weights')
        serializer.save(path=summarized_weights_path, weights=weights)

        self.summarized_weights_path = summarized_weights_path


if __name__ == '__main__':
    session = SummarizeSession.run()
