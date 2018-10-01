from abc import abstractmethod, ABC
from collections import Iterable
from logging import getLogger

from tatau_core.nn.tatau.dataset import NumpyChunkedDataset, AutoDataLoader
from tatau_core.utils.class_loader import load_class
from .progress import TrainProgress

logger = getLogger('tatau_core')


class Model(ABC):
    """
    Tatau NN Model
    """

    weights_summarizer_class = 'tatau_core.nn.tatau.summarizer.Summarizer'
    weights_serializer_class = 'tatau_core.nn.tatau.serializer.WeightsSerializer'

    transform_train = None
    transform_eval = None

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

    def data_preprocessing(self, chunk_dirs: Iterable, batch_size, transform: callable) -> Iterable:
        return AutoDataLoader(
            dataset=NumpyChunkedDataset(chunk_dirs=chunk_dirs, transform=transform),
            batch_size=batch_size, shuffle=True, pin_memory=False)

    @abstractmethod
    def train(self, chunk_dirs: Iterable, batch_size: int, current_iteration: int,
              nb_epochs: int, train_progress: TrainProgress)-> list:
        """
        Train model
        :param train_progress: Task Progress Callback
        :param batch_size: batch_size
        :param chunk_dirs: chunk dirs
        :param current_iteration: iteration
        :param nb_epochs: number of epochs
        :return: loss history list((loss, acc))
        """
        pass

    @abstractmethod
    def eval(self, chunk_dirs: Iterable):
        """
        Evaluate  model
        :param chunk_dirs: chunk dirs
        :return: tuple(loss, acc)
        """
        raise NotImplementedError()

    def adjust_learning_rate(self, epoch: int):
        """
        Adjust learning rate over training process
        :param epoch: training epoch
        :return:
        """
        pass

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
