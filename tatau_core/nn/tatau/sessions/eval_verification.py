from collections import deque
from logging import getLogger

from tatau_core.models import VerificationAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.sessions.eval_train import TrainEvalSession
from tatau_core.utils.ipfs import Directory

logger = getLogger(__name__)


class VerificationEvalSession(TrainEvalSession):
    def process_assignment(self, assignment: VerificationAssignment, *args, **kwargs):
        dirs, files = Directory(assignment.verification_data.test_dir_ipfs).ls()

        x_test = deque()
        y_test = deque()

        for ipfs_file in files:
            if ipfs_file.name[0] == 'x':
                x_test.append(ipfs_file.multihash)
                continue

            if ipfs_file.name[0] == 'y':
                y_test.append(ipfs_file.multihash)

        loss, accuracy = self._run_eval(
            task_declaration_id=assignment.task_declaration_id,
            model_ipfs=assignment.verification_data.model_code_ipfs,
            current_iteration=assignment.verification_data.current_iteration,
            weights_ipfs=assignment.verification_result.weights,
            x_files_ipfs=x_test,
            y_files_ipfs=y_test
        )

        assignment.verification_result.loss = loss
        assignment.verification_result.accuracy = accuracy

    def main(self):
        logger.info('Run verification evaluation')
        model = Model.load_model(path=self.load_model_path())
        model.load_weights(self._load_weights_path())
        loss, acc = model.eval(x_path_list=self.load_x_test(), y_path_list=self.load_y_test())
        self.save_eval_result(loss=loss, acc=acc)


if __name__ == '__main__':
    session = VerificationEvalSession.run()
