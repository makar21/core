from tatau_core.nn.models.keras import KerasModel
from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout
import numpy
from keras.utils import np_utils


class Model(KerasModel):
    @classmethod
    def native_model_factory(cls):
        model = Sequential()
        model.add(Dense(512, input_shape=(784,)))
        model.add(Activation('relu'))

        # An "activation" is just a non-linear function applied to the output
        # of the layer above. Here, with a "rectified linear unit",
        # we clamp all values below 0 to 0.

        model.add(Dropout(0.2))  # Dropout helps protect the model from memorizing or "overfitting" the training data
        model.add(Dense(512))
        model.add(Activation('relu'))
        model.add(Dropout(0.2))
        model.add(Dense(10))
        model.add(Activation('softmax'))

        # This special "softmax" activation among other things,
        # ensures the output is a valid probaility distribution, that is
        # that its values are all non-negative and sum to 1.

        model.compile(loss='categorical_crossentropy', optimizer='adam')
        return model

    @classmethod
    def data_preprocessing(cls, x: numpy.array, y: numpy.array):
        x = x.reshape(len(x), 784).astype('float32') / 255
        y = np_utils.to_categorical(y, 10)
        return x, y



