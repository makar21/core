import time
from abc import ABCMeta
from collections import Iterable
from logging import getLogger

import torch
# noinspection PyUnresolvedReferences
from torch import cuda, from_numpy
from torch.nn import DataParallel

from tatau_core.nn.tatau import model, TrainProgress

logger = getLogger('tatau_core')


class Model(model.Model, metaclass=ABCMeta):
    weights_serializer_class = 'tatau_core.nn.torch.serializer.WeightsSerializer'
    weights_summarizer_class = 'tatau_core.nn.torch.summarizer.Median'

    transform_train = None
    transform_eval = None

    def __init__(self, optimizer_class, optimizer_kwargs, criterion):
        super(Model, self).__init__()

        self._optimizer_kwargs = optimizer_kwargs

        self._device = 'cuda' if torch.cuda.is_available() else 'cpu'

        logger.info("Model device: {}".format(self.device))

        self._model = self.native_model_factory()
        self._gpu_count = torch.cuda.device_count() if torch.cuda.is_available() else 0

        logger.info("GPU count: {}".format(self._gpu_count))

        self._model = DataParallel(self._model)

        self._model = self._model.to(self.device)

        self._criterion = criterion.to(self.device)

        self._optimizer = optimizer_class(self._model.parameters(), **optimizer_kwargs)

    @property
    def device(self):
        return self._device

    @property
    def optimizer(self):
        return self._optimizer

    @property
    def criterion(self):
        return self._criterion

    @classmethod
    def native_model_factory(cls):
        raise NotImplementedError()

    def get_weights(self):
        state = {
            'weights': self.native_model.state_dict(),
            'optimizer': self.optimizer.state_dict(),
            # 'criterion': self._criterion.state_dict()
        }
        return state

    def set_weights(self, weights: dict):
        self.native_model.load_state_dict(weights['weights'])
        self.optimizer.load_state_dict(weights['optimizer'])
        # self._criterion.load_state_dict(weights['criterion'])

    def train(self, chunk_dirs: Iterable, batch_size: int, current_iteration: int,
              nb_epochs: int, train_progress: TrainProgress):

        self.native_model.train()

        batch_size = batch_size * max(1, self._gpu_count)
        logger.info("Batch size: {}".format(batch_size))

        # dataset = self.data_preprocessing(chunk_dirs=chunk_dirs, transforms=self.transforms_train)

        loader = self.data_preprocessing(chunk_dirs=chunk_dirs, batch_size=batch_size, transform=self.transform_train)
        # DataLoader(dataset, batch_size=batch_size, shuffle=False, num_workers=0, pin_memory=False)

        train_history = {'loss': [], 'acc': []}
        for epoch in range(1, nb_epochs + 1):
            epoch_started_at = time.time()
            nb_epoch = (current_iteration - 1) * nb_epochs + epoch
            self.adjust_learning_rate(nb_epoch)
            epoch_loss = 0.0
            correct = 0
            batch_started_at = time.time()
            for batch_idx, (input_, target) in enumerate(loader, 0):
                if self._gpu_count:
                    input_, target = input_.to(self.device), target.to(self.device)
                self.optimizer.zero_grad()
                output = self.native_model(input_)
                loss = self._criterion(output, target)
                loss_item = loss.item()
                epoch_loss += loss_item
                # noinspection PyUnresolvedReferences
                _, predicted = torch.max(output.data, 1)
                correct += predicted.eq(target).sum().item()
                loss.backward()
                self.optimizer.step()
                batch_finished_at = time.time()
                batch_time = batch_finished_at - batch_started_at
                batch_started_at = batch_finished_at
                logger.info(
                    'Train Epoch: {epoch} [{it}/{total_it} ({progress:.0f}%)]\tLoss: {loss:.4f}\tTime: {time:.2f} secs'.format(
                        epoch=epoch,
                        it=(batch_idx + 1) * len(input_),
                        total_it=len(loader.dataset),
                        progress=100. * batch_idx / len(loader),
                        loss=epoch_loss / (batch_idx + 1),
                        time=batch_time
                    ))
            epoch_time = time.time() - epoch_started_at
            epoch_loss = epoch_loss / len(loader)
            epoch_acc = correct / len(loader.dataset)
            logger.info("Epoch #{}: Loss: {:.4f} Acc: {:.2f} Time: {:.2f} secs".format(
                nb_epoch, epoch_loss, 100 * epoch_acc, epoch_time))
            train_history['loss'].append(epoch_loss)
            train_history['acc'].append(epoch_acc)
        return train_history

    def eval(self, chunk_dirs: Iterable):
        # noinspection PyUnresolvedReferences
        # from torch import from_numpy
        self.native_model.eval()
        test_loss = 0
        correct = 0

        # dataset = self.data_preprocessing(x_path_list, y_path_list, self.transforms_eval)
        # loader = DataLoader(dataset, batch_size=128, shuffle=False, num_workers=0)

        loader = self.data_preprocessing(chunk_dirs=chunk_dirs, batch_size=128, transform=self.transform_eval)

        with torch.no_grad():
            for input_, target in loader:
                if self._gpu_count:
                    input_, target = input_.to(self.device), target.to(self.device)
                outputs = self.native_model(input_)
                loss = self._criterion(outputs, target)
                test_loss += loss.item()
                # noinspection PyUnresolvedReferences
                _, predicted = torch.max(outputs.data, 1)
                correct += predicted.eq(target).sum().item()
                # correct += (predicted == target).sum().item()

        test_loss /= len(loader)
        test_acc = correct / len(loader.dataset)
        logger.info('\nTest set: Average loss: {:.8f}, Accuracy: {}/{} ({:.4f}%)\n'.format(
            test_loss, correct, len(loader.dataset), test_acc))
        return test_loss, test_acc
