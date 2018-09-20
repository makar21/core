import os
from collections import deque
from logging import getLogger

from tatau_core.models import TaskAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.utils.ipfs import Downloader
from .session import Session

logger = getLogger(__name__)


class TrainEvalSession(Session):
    def __init__(self, uuid=None):
        super(TrainEvalSession, self).__init__(module=__name__, uuid=uuid)

    @property
    def _weights_path(self):
        return os.path.join(self.base_dir, 'weights.pkl')

    def _save_weights_path(self, path):
        self.save_object(path=self._weights_path, obj=path)

    def _load_weights_path(self):
        return self.load_object(path=self._weights_path)

    @property
    def _eval_result_path(self):
        return os.path.join(self.base_dir, 'eval.pkl')

    def save_eval_result(self, loss, acc):
        self.save_object(path=self._eval_result_path, obj=dict(loss=loss, acc=acc))

    def load_eval_result(self):
        result = self.load_object(path=self._eval_result_path)
        return result['loss'], result['acc']

    def _run_eval(self, task_declaration_id, model_ipfs, current_iteration, weights_ipfs, x_files_ipfs, y_files_ipfs):
        assert len(x_files_ipfs) == len(y_files_ipfs)
        if len(x_files_ipfs) == 0:
            # this worker is not involved in the evaluation
            return

        downloader = Downloader(task_declaration_id)
        downloader.add_to_download_list(model_ipfs, 'model.py')

        weights_file_name = 'eval_weights_{}'.format(current_iteration)
        downloader.add_to_download_list(weights_ipfs, weights_file_name)

        x_test_paths = deque()
        y_test_paths = deque()

        for index, x_ipfs in enumerate(x_files_ipfs):
            y_ipfs = y_files_ipfs[index]

            x_file_name = 'x_test_{}'.format(index)
            y_file_name = 'y_test_{}'.format(index)

            downloader.add_to_download_list(x_ipfs, x_file_name)
            x_test_paths.append(downloader.resolve_path(x_file_name))
            downloader.add_to_download_list(y_ipfs, y_file_name)
            y_test_paths.append(downloader.resolve_path(y_file_name))

        downloader.download_all()

        self.save_model_path(downloader.resolve_path('model.py'))
        self._save_weights_path(downloader.resolve_path(weights_file_name))
        self.save_x_test(x_test_paths)
        self.save_y_test(y_test_paths)

        self._run()

        return self.load_eval_result()

    def process_assignment(self, assignment: TaskAssignment, *args, **kwargs):
        iteration = assignment.train_data.current_iteration - 1
        loss, accuracy = self._run_eval(
            task_declaration_id=assignment.task_declaration_id,
            model_ipfs=assignment.train_data.model_code,
            current_iteration=iteration,
            weights_ipfs=assignment.train_data.initial_weights,
            x_files_ipfs=assignment.train_data.x_test,
            y_files_ipfs=assignment.train_data.y_test
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

        model = Model.load_model(path=self.load_model_path())
        model.load_weights(self._load_weights_path())

        loss, acc = model.eval(x_path_list=self.load_x_test(), y_path_list=self.load_y_test())
        self.save_eval_result(loss=loss, acc=acc)


if __name__ == '__main__':
    session = TrainEvalSession.run()
