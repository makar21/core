from logging import getLogger

from tatau_core.models import VerificationAssignment
from tatau_core.nn.tatau.model import Model
from tatau_core.nn.tatau.sessions.eval_train import TrainEvalSession
from tatau_core.utils.ipfs import Directory

logger = getLogger('tatau_core')


class VerificationEvalSession(TrainEvalSession):
    def process_assignment(self, assignment: VerificationAssignment, *args, **kwargs):
        dirs, files = Directory(assignment.verification_data.test_dir_ipfs).ls()

        loss, accuracy = self._run_eval(
            task_declaration_id=assignment.task_declaration_id,
            model_ipfs=assignment.verification_data.model_code_ipfs,
            current_iteration=assignment.verification_data.current_iteration,
            weights_ipfs=assignment.verification_result.weights_ipfs,
            test_chunks_ipfs=[d.multihash for d in dirs]
        )

        assignment.verification_result.loss = loss
        assignment.verification_result.accuracy = accuracy

    def main(self):
        logger.info('Run verification evaluation')
        model = Model.load_model(path=self.model_path)
        model.load_weights(self.weights_path)
        loss, acc = model.eval(chunk_dirs=self.chunk_dirs)
        self.eval_results = {
            'loss': loss,
            'acc': acc
        }


if __name__ == '__main__':
    session = VerificationEvalSession.run()
