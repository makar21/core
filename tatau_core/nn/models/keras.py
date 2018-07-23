import numpy
from .tatau import TatauModel, TrainProgress
from keras.models import Sequential
from keras.callbacks import Callback


class ProgressCallback(Callback):
    def __init__(self, nb_epochs: int, train_progress: TrainProgress):
        super().__init__()
        self.nb_epochs = nb_epochs
        self.train_progress = train_progress

    def on_epoch_end(self, epoch, logs=None):
        progress = int(epoch * 100.0 / self.nb_epochs)
        self.train_progress.progress_callback(progress)

    def on_train_end(self, logs=None):
        self.train_progress.progress_callback(100)


class KerasModel(TatauModel):

    @classmethod
    def native_model_factory(cls):
        """
        Construct Keras Sequential
        :return: model
        :rtype: Sequential
        """
        raise NotImplementedError()

    def get_weights(self):
        return self.native_model.get_weights()

    def set_weights(self, weights: list):
        self.native_model.set_weights(weights=weights)

    @classmethod
    def data_preprocessing(cls, x: numpy.array, y: numpy.array):
        return x, y

    def train(self, x: numpy.array, y: numpy.array, batch_size: int, nb_epochs: int, train_progress: TrainProgress):
        callbacks = [
            ProgressCallback(nb_epochs=nb_epochs, train_progress=train_progress)
        ]

        x, y = self.data_preprocessing(x, y)

        history = self.native_model.fit(
            x=x, y=y, batch_size=batch_size, epochs=nb_epochs, verbose=1, callbacks=callbacks)

        return history

    def eval(self, x: numpy.array, y: numpy.array):
        x, y = self.data_preprocessing(x, y)
        return self.native_model.evaluate(x=x, y=y, verbose=1)
