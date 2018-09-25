import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import traceback
from abc import ABCMeta
from logging import getLogger
from uuid import uuid4

from tatau_core.metrics import MetricsCollector

logger = getLogger(__name__)


class SessionValue:
    def __init__(self):
        self._name = None

    def _make_path(self, base_dir):
        return os.path.join(base_dir, self._name + '.pkl')

    def __get__(self, instance, owner):
        path = self._make_path(instance.base_dir)
        if not os.path.exists(path):
            return None
        
        with open(self._make_path(instance.base_dir), 'rb') as f:
            return pickle.load(f)

    def __set__(self, instance, value):
        with open(self._make_path(instance.base_dir), 'wb') as f:
            pickle.dump(value, f, pickle.HIGHEST_PROTOCOL)


class ExceptionValue(SessionValue):
    def __set__(self, instance, value):
        assert isinstance(value, Exception)
        with open(self._make_path(instance.base_dir), 'wb') as f:
            pickle.dump({
                'exception': value,
                'traceback': traceback.format_tb(value.__traceback__)
            }, f, pickle.HIGHEST_PROTOCOL)


class SessionBase(ABCMeta):
    """Metaclass for all sesstions."""
    def __new__(mcs, name, bases, attrs):
        super_new = super().__new__
        # Create the class.
        new_class = super_new(mcs, name, bases, attrs)
        for name, attr_obj in attrs.items():
            if isinstance(attr_obj, SessionValue):
                attr_obj._name = name
        return new_class


class Session(metaclass=SessionBase):
    model_path = SessionValue()
    exception = ExceptionValue()

    def __init__(self, module, uuid=None):
        self._uuid = uuid or str(uuid4())
        self._module = module
        self._metrics_collector = MetricsCollector()
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

    def get_tflops(self):
        return self._metrics_collector.get_tflops()

    def clean(self):
        logger.info('Cleanup session {}'.format(self.uuid))
        shutil.rmtree(self.base_dir)
        logger.info('Base dir {} is removed'.format(self.base_dir))
        self._metrics_collector.clean()

    def process_assignment(self, assignment):
        raise NotImplementedError()

    def _run(self, *args, async=False):

        args_list = ['python', '-m', self._module, self.uuid]
        args_list += [str(a) for a in args]

        if async:
            subprocess.Popen(args_list)
            return

        self._metrics_collector.start_and_wait_signal()
        with subprocess.Popen(args_list) as process:
            self._metrics_collector.set_pid(process.pid)
            with self._metrics_collector:
                process.wait()

        exception = self.exception
        if exception:
            raise RuntimeError('{}'.format(exception))

    def main(self):
        raise NotImplementedError()

    @classmethod
    def run(cls):
        session = cls(uuid=sys.argv[1])
        try:
            session.main()
        except Exception as ex:
            session.exception = ex
            exit(1)
