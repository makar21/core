import os
from logging import getLogger
from examples.torch.imagenet12.resnet import Model
from tatau_core.nn.torch.model import TrainProgress
from glob import glob


logger = getLogger(__name__)


def train_local(x_train_paths, y_train_paths, x_test_paths, y_test_paths, model_path, batch_size, epochs):

    model = Model.load_model(path=model_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    history = model.train(x_train_paths, y_train_paths,
                          batch_size=batch_size, nb_epochs=epochs,
                          current_iteration=1,
                          train_progress=LocalProgress())

    print(history)
    loss, acc = model.eval(x_test_paths, y_test_paths)
    print('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def main():
    model_path = 'resnet.py'
    base_path = 'data/'

    x_train_paths = glob(os.path.join(base_path, 'x_train', '*.npz'))
    y_train_paths = glob(os.path.join(base_path, 'y_train', '*.npz'))
    x_test_paths = glob(os.path.join(base_path, 'x_test', '*.npz'))
    y_test_paths = glob(os.path.join(base_path, 'y_test', '*.npz'))

    train_local(x_train_paths, y_train_paths, x_test_paths, y_test_paths, model_path, batch_size=32, epochs=1)


if __name__ == '__main__':
    main()
