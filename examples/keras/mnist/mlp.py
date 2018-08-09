from tatau_core.nn.keras import model
import keras
from keras.layers import Dense, Activation, Dropout
import numpy
from keras.utils import np_utils


class Model(model.Model):
    @classmethod
    def native_model_factory(cls) -> keras.models.Model:
        net = keras.models.Sequential()
        net.add(Dense(512, input_shape=(784,)))
        net.add(Activation('relu'))

        # An "activation" is just a non-linear function applied to the output
        # of the layer above. Here, with a "rectified linear unit",
        # we clamp all values below 0 to 0.

        net.add(Dropout(0.2))  # Dropout helps protect the model from memorizing or "overfitting" the training data
        net.add(Dense(512))
        net.add(Activation('relu'))
        net.add(Dropout(0.2))
        net.add(Dense(10))
        net.add(Activation('softmax'))

        # This special "softmax" activation among other things,
        # ensures the output is a valid probaility distribution, that is
        # that its values are all non-negative and sum to 1.

        net.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
        return net

    @classmethod
    def data_preprocessing(cls, x: numpy.array, y: numpy.array):
        x = x.reshape(len(x), 784).astype('float32') / 255
        y = np_utils.to_categorical(y, 10)
        return x, y



