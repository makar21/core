from logging import getLogger
from uuid import uuid4
import tempfile
import os
import shutil
import subprocess
from abc import ABC
import pickle

logger = getLogger(__name__)


class Session(ABC):
    def __init__(self, module, uuid=None):
        self._uuid = uuid or str(uuid4())
        self._module = module
        logger.info("Init {}: {}".format(self.__class__.__name__, self.uuid))

    @property
    def uuid(self):
        return self._uuid

    @property
    def base_dir(self):
        session_dir = os.path.join(tempfile.gettempdir(), self.uuid)
        if not os.path.exists(session_dir):
            os.mkdir(session_dir)
        return session_dir

    @property
    def model_path(self):
        return os.path.join(self.base_dir, "model.py")

    @property
    def x_train_path(self):
        return os.path.join(self.base_dir, "x_train.npy")

    @property
    def y_train_path(self):
        return os.path.join(self.base_dir, "y_train.npy")

    @property
    def init_weights_path(self):
        return os.path.join(self.base_dir, "init_weights.pkl")

    def get_tflops(self):
        return self._metrics_collector.get_tflops()

    def clean(self):
        shutil.rmtree(self.base_dir)
        self._metrics_collector.clean()

    def process_assignment(self, assignment):
        raise NotImplementedError()

    def _run(self, *args):
        args_list = ["python", "-m", self._module, self.uuid]
        args_list += [str(a) for a in args]

        self._metrics_collector.start_and_wait_signal()
        with subprocess.Popen(args_list) as process:
            self._metrics_collector.set_pid(process.pid)
            with self._metrics_collector:
                process.wait()

    def main(self):
        raise NotImplementedError()

    @classmethod
    def save_object(cls, path, obj):
        with open(path, "wb") as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load_object(cls, path):
        with open(path, "rb") as f:
            obj = pickle.load(f)
        return obj


