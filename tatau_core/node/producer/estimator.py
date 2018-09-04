from tatau_core.models import EstimationAssignment
from tatau_core.utils.ipfs import Directory


class Estimator:
    @staticmethod
    def get_data_for_estimate(task_declaration):
        dataset = task_declaration.dataset
        ipfs_dir = Directory(dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()
        estimate_data = {
            'x_train': None,
            'y_train': None,
            'model_code': task_declaration.train_model.code_ipfs,
            'batch_size': task_declaration.batch_size,
            'initial_weights': task_declaration.weights
        }

        y_file_name = None
        for f in files:
            if estimate_data['x_train'] and estimate_data['y_train']:
                break

            if estimate_data['x_train'] is None and f.name[0] == 'x':
                y_file_name = 'y' + f.name[1:]
                estimate_data['x_train'] = f.multihash
                continue

            if f.name == y_file_name:
                estimate_data['y_train'] = f.multihash
                continue

        return estimate_data

    @staticmethod
    def estimate(task_declaration):
        estimation_assignments = task_declaration.get_estimation_assignments(
            states=(
                EstimationAssignment.State.DATA_IS_READY,
                EstimationAssignment.State.IN_PROGRESS,
                EstimationAssignment.State.FINISHED
            )
        )

        failed = False

        assert len(estimation_assignments)

        sum_tflops = 0.0
        for estimation_assignment in estimation_assignments:
            assert estimation_assignment.state == EstimationAssignment.State.FINISHED
            sum_tflops += estimation_assignment.tflops
            if estimation_assignment.error is not None:
                failed = True
                return 0.0, failed

        av_tflops = sum_tflops / len(estimation_assignments)
        ipfs_dir = Directory(task_declaration.dataset.train_dir_ipfs)
        dirs, files = ipfs_dir.ls()
        batch_count = len(files) / 2
        return av_tflops * batch_count * task_declaration.epochs, failed


