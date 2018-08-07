from keras.callbacks import Callback
from tatau_core.nn.tatau import progress


class ProgressCallback(Callback):
    def __init__(self, nb_epochs: int, train_progress: progress.TrainProgress):
        super().__init__()
        self.nb_epochs = nb_epochs
        self.train_progress = train_progress

    def on_epoch_end(self, epoch, logs=None):
        progress = int(epoch * 100.0 / self.nb_epochs)
        self.train_progress.progress_callback(progress)

    def on_train_end(self, logs=None):
        self.train_progress.progress_callback(100)