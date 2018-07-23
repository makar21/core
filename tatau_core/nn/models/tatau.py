import numpy
from abc import abstractmethod, ABC
from logging import getLogger

logger = getLogger(__name__)


class TrainProgress:
    def progress_callback(self, progress):
        pass


class TatauModel(ABC):
    """
    Tatau NN Model
    """

    @classmethod
    def load_model(cls, path):
        """
        Construct model from asset
        :param path: model path
        :return: model instance
        :rtype: TatauModel
        """
        model = None

        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("model", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            assert hasattr(module, 'Model')
            assert issubclass(module.Model, TatauModel)
            model = module.Model()
        except Exception as e:
            logger.exception(e)
        return model

    def __init__(self):
        self._model = self.native_model_factory()

    @property
    def native_model(self):
        return self._model

    @classmethod
    def native_model_factory(cls):
        raise NotImplementedError()

    @abstractmethod
    def get_weights(self):
        """
        Get model weights
        :return: weights
        :rtype: list(numpy.array)
        """
        pass

    @abstractmethod
    def set_weights(self, weights: list):
        """
        Set model weights
        :param weights: weights
        :return:
        """
        pass

    @abstractmethod
    def train(self, x: numpy.array, y: numpy.array, batch_size: int, nb_epochs: int, train_progress: TrainProgress):
        """
        Train model
        :param train_progress: Task Progress Callback
        :param batch_size: batch_size
        :param x: train inputs
        :param y: train outputs
        :param nb_epochs: number of epochs
        :return: loss history list((loss, acc))
        """
        pass

    @abstractmethod
    def eval(self, x: numpy.array, y: numpy.array):
        """
        Evaluate  model
        :param x: inputs
        :param y: outputs
        :return: tuple(loss, acc)
        """
        raise NotImplementedError()
