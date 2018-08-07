import numpy
from tatau_core.nn import tatau
from .progress import ProgressCallback


class Model(tatau.Model):

    # noinspection PyMethodMayBeStatic
    def get_keras_callbacks(self):
        return []

    @classmethod
    def native_model_factory(cls):
        """
        Construct Keras Model
        :return: model
        :rtype: keras.models.Model
        """
        raise NotImplementedError()

    def get_weights(self):
        return self.native_model.get_weights()

    def set_weights(self, weights: list):
        self.native_model.set_weights(weights=weights)

    @classmethod
    def data_preprocessing(cls, x: numpy.array, y: numpy.array):
        return x, y

    def train(self, x: numpy.array, y: numpy.array, batch_size: int, nb_epochs: int, train_progress: tatau.TrainProgress):
        callbacks = [
            ProgressCallback(nb_epochs=nb_epochs, train_progress=train_progress)
        ]

        callbacks.extend(self.get_keras_callbacks())

        x, y = self.data_preprocessing(x, y)

        history = self.native_model.fit(
            x=x, y=y, batch_size=batch_size, epochs=nb_epochs, verbose=1, callbacks=callbacks)

        train_history = dict()

        for metric in history.history.keys():
            train_history[metric] = [float(val) for val in history.history[metric]]

        return train_history

    def eval(self, x: numpy.array, y: numpy.array):
        x, y = self.data_preprocessing(x, y)
        return self.native_model.evaluate(x=x, y=y, verbose=1)


