import numpy as np
import os
# import torchvision.transforms as transforms
from logging import getLogger
from tatau_core.nn.tatau.model import Model, TrainProgress


logger = getLogger(__name__)

# normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])


def train_local(x_train_paths, y_train_paths,
                x_test_paths, y_test_paths,
                model_path, batch_size, epochs):
    model = Model.load_model(path=model_path)

    xs_train = [np.load(path)['arr_0'] for path in x_train_paths]
    ys_train = [np.load(path)['arr_0'] for path in y_train_paths]
    x_train = np.concatenate(xs_train, axis=0)
    y_train = np.concatenate(ys_train)
    xs_test = [np.load(path)['arr_0'] for path in x_test_paths]
    ys_test = [np.load(path)['arr_0'] for path in y_test_paths]
    x_test = np.concatenate(xs_test, axis=0)
    y_test = np.concatenate(ys_test)

    # normalize training data
    x_train = x_train / 255.
    x_test = x_test / 255.

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    history = model.train(x=x_train, y=y_train, batch_size=batch_size, nb_epochs=epochs, train_progress=LocalProgress())

    print(history)
    loss, acc = model.eval(x=x_test, y=y_test)
    print('loss({}):{}, acc({}):{}'.format(loss.__class__.__name__, loss, acc.__class__.__name__, acc))


def main():
    model_path = 'resnet.py'

    base_path = '../../../../../amazon-sagemaker-vs-tatau/pytorch_imagenet/compressed'
    train_chunks_num = 1
    test_chunks_num = 1

    x_train_paths_pattern = os.path.join(base_path, 'x_train')
    y_train_paths_pattern = os.path.join(base_path, 'y_train')
    x_test_paths_pattern = os.path.join(base_path, 'x_test')
    y_test_paths_pattern = os.path.join(base_path, 'y_test')

    x_train_paths = [os.path.join(x_train_paths_pattern, f'x_train_part_{p:05}.npz') for p in range(train_chunks_num)]
    y_train_paths = [os.path.join(y_train_paths_pattern, f'y_train_part_{p:05}.npz') for p in range(train_chunks_num)]

    x_test_paths = [os.path.join(x_test_paths_pattern, f'x_test_part_{p:05}.npz') for p in range(test_chunks_num)]
    y_test_paths = [os.path.join(y_test_paths_pattern, f'y_test_part_{p:05}.npz') for p in range(test_chunks_num)]

    batch_size = 32
    epochs = 10

    train_local(x_train_paths, y_train_paths, x_test_paths, y_test_paths, model_path, batch_size, epochs)


if __name__ == '__main__':
    main()
