import os
import torchvision.transforms as transforms
from logging import getLogger
from tatau_core.nn.tatau.model import Model, TrainProgress
from tatau_core.nn.torch.data_loader import NumpyDataChunk
from torch.utils.data import ConcatDataset


logger = getLogger(__name__)

normalize = transforms.Compose([transforms.ToTensor(),
                                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])


def train_local(train_dataset, test_dataset, model_path, batch_size, epochs, num_workers):

    model = Model.load_model(path=model_path)

    class LocalProgress(TrainProgress):
        def progress_callback(self, progress):
            logger.info("Progress: {:.2f}".format(progress))

    history = model.train(train_dataset, num_workers=num_workers, batch_size=batch_size, nb_epochs=epochs,
                          train_progress=LocalProgress())

    print(history)
    loss, acc = model.eval(test_dataset, num_workers=num_workers)
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

    print('Train data loading and preprocessing...')
    train_chunks = [NumpyDataChunk(x_path, y_path, transform=normalize)
                    for x_path, y_path in zip(x_train_paths, y_train_paths)]
    train_dataset = ConcatDataset(train_chunks)
    print(f'loaded {len(train_dataset)} of training samples')

    print('Test data loading and preprocessing...')
    test_chunks = [NumpyDataChunk(x_path, y_path, transform=normalize)
                   for x_path, y_path in zip(x_test_paths, y_test_paths)]
    test_dataset = ConcatDataset(test_chunks)
    print(f'loaded {len(test_dataset)} of testing samples')

    train_local(train_dataset, test_dataset, model_path, batch_size=32, epochs=1, num_workers=8)


if __name__ == '__main__':
    main()
