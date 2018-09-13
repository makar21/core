import os
from logging import getLogger
from examples.torch.imagenet12.model import Model
from tatau_core.nn.torch.model import TrainProgress


logger = getLogger(__name__)


def train_local(train_dataset, test_dataset, model_path, batch_size, epochs):

    model = Model.load_model(path=model_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    history = model.train(train_dataset, batch_size=batch_size, nb_epochs=epochs,
                          train_progress=LocalProgress())

    print(history)
    loss, acc = model.eval(test_dataset)
    print('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def main():
    model_path = 'resnet.py'
    base_path = '../../../../../amazon-sagemaker-vs-tatau/pytorch_imagenet/compressed'

    x_train_paths_pattern = os.path.join(base_path, 'x_train')
    y_train_paths_pattern = os.path.join(base_path, 'y_train')

    train_chunks_number = 80
    test_chunks_number = 8
    x_train_paths = [os.path.join(x_train_paths_pattern, f'x_train_part_{p:05}.npz') for p in range(train_chunks_number)]
    y_train_paths = [os.path.join(y_train_paths_pattern, f'y_train_part_{p:05}.npz') for p in range(train_chunks_number)]

    x_test_paths = [os.path.join(base_path, 'x_test', f'x_test_part_{p:05}.npz') for p in range(test_chunks_number)]
    y_test_paths = [os.path.join(base_path, 'y_test', f'y_test_part_{p:05}.npz') for p in range(test_chunks_number)]

    train_local(x_train_paths, y_train_paths, x_test_paths, y_test_paths, model_path, batch_size=32, epochs=1)


if __name__ == '__main__':
    main()
