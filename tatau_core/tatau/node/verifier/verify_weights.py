import os
import shutil
import tempfile
from logging import getLogger

from tatau_core.nn.models.tatau import TatauModel
from tatau_core.utils.ipfs import IPFS
from verifier.verify_results import verify

logger = getLogger()


def download_model_code(target_dir, model_code_ipfs):
    ipfs = IPFS()

    model_code = ipfs.read(model_code_ipfs)
    model_code_path = os.path.join(target_dir, '{}.py'.format(model_code_ipfs))

    with open(model_code_path, 'wb') as f:
        f.write(model_code)

    return model_code_path


def verify_train_results(train_results, model_code_ipfs):
    logger.info('Verify results: {}'.format(train_results))
    target_dir = tempfile.mkdtemp()
    try:
        ipfs = IPFS()

        model_code_path = download_model_code(target_dir, model_code_ipfs)
        model_obj = TatauModel.load_model(path=model_code_path)

        downloaded_results = []
        for worker_result in train_results:
            if worker_result['result'] is None:
                # TODO: handle this
                pass
            else:
                downloaded_results.append({
                    'worker_id': worker_result['worker_id'],
                    'file_path': ipfs.download(worker_result['result'], target_dir)
                })

        return verify(downloaded_results, model_obj)
    finally:
        shutil.rmtree(target_dir)
