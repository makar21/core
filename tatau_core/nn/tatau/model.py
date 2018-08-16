import numpy
from abc import abstractmethod, ABC
from logging import getLogger
from .progress import TrainProgress
from tatau_core.utils.class_loader import load_class


logger = getLogger(__name__)


class Model(ABC):
    """
    Tatau NN Model
    """

    weights_summarizer_class = 'tatau_core.nn.tatau.summarizer.Summarizer'
    weights_serializer_class = 'tatau_core.nn.tatau.serializer.WeightsSerializer'

    @classmethod
    def load_model(cls, path):
        """
        Construct model from asset
        :param path: model path
        :return: model instance
        :rtype: Model
        """
        model = None

        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("model", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            assert hasattr(module, 'Model')
            assert issubclass(module.Model, Model)
            model = module.Model()
        except Exception as e:
            logger.exception(e)
        return model

    def __init__(self):
        self._model = None

    @property
    def native_model(self):
        if not self._model:
            self._model = self.native_model_factory()
        return self._model

    @classmethod
    def native_model_factory(cls):
        """
        Native Model Factory
        :return: native model
        """
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

    @classmethod
    def get_weights_serializer(cls):
        return load_class(cls.weights_serializer_class)()

    @classmethod
    def get_weights_summarizer(cls):
        return load_class(cls.weights_summarizer_class)()

    def load_weights(self, path: str):
        self.set_weights(weights=self.get_weights_serializer().load(path=path))

    def save_weights(self, path: str):
        self.get_weights_serializer().save(weights=self.get_weights(), path=path)