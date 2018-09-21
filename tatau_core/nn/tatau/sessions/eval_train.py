from collections import deque
from logging import getLogger

from tatau_core.models import TaskAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.utils.ipfs import Downloader
from .session import Session, SessionValue

logger = getLogger(__name__)


class TrainEvalSession(Session):
    weights_path = SessionValue()
    chunk_dirs = SessionValue()
    eval_results = SessionValue()

    def __init__(self, uuid=None):
        super(TrainEvalSession, self).__init__(module=__name__, uuid=uuid)

    def _run_eval(self, task_declaration_id, model_ipfs, current_iteration, weights_ipfs, test_chunks_ipfs):
        if len(test_chunks_ipfs) == 0:
            # this worker is not involved in the evaluation
            return None, None

        downloader = Downloader(task_declaration_id)
        downloader.add_to_download_list(model_ipfs, 'model.py')

        weights_file_name = 'eval_weights_{}'.format(current_iteration)
        downloader.add_to_download_list(weights_ipfs, weights_file_name)

        chunk_dirs = deque()
        for index, chunk_ipfs in enumerate(test_chunks_ipfs):
            dir_name = '{}_chunk_test_{}'.format(current_iteration, index)
            downloader.add_to_download_list(chunk_ipfs, dir_name)
            chunk_dirs.append(downloader.resolve_path(dir_name))

        downloader.download_all()

        self.model_path = downloader.resolve_path('model.py')
        self.weights_path = downloader.resolve_path(weights_file_name)
        self.chunk_dirs = chunk_dirs

        self._run()

        eval_results = self.eval_results
        return eval_results['loss'], eval_results['acc']

    def process_assignment(self, assignment: TaskAssignment, *args, **kwargs):
        if len(assignment.train_data.test_chunks_ipfs) == 0:
            # this worker is not involved in the evaluation
            return

        iteration = assignment.train_data.current_iteration - 1
        loss, accuracy = self._run_eval(
            task_declaration_id=assignment.task_declaration_id,
            model_ipfs=assignment.train_data.model_code_ipfs,
            current_iteration=iteration,
            weights_ipfs=assignment.train_data.initial_weights_ipfs,
            test_chunks_ipfs=assignment.train_data.test_chunks_ipfs
        )

        logger.info('loss: {}, accuracy: {}'.format(loss, accuracy))

        if assignment.train_result.eval_results is None:
            assignment.train_result.eval_results = {}

        assignment.train_result.eval_results[str(iteration)] = {
            'loss': loss,
            'accuracy': accuracy
        }

        assignment.train_result.save()

    def main(self):
        logger.info('Run evaluation')

        model = Model.load_model(path=self.model_path)
        model.load_weights(self.weights_path)

        loss, acc = model.eval(chunk_dirs=self.chunk_dirs)
        self.eval_results = {
            'loss': loss,
            'acc': acc
        }


if __name__ == '__main__':
    session = TrainEvalSession.run()
