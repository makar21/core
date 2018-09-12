from tatau_core.nn.tatau import model
from tatau_core.nn.tatau import TrainProgress
from torch.utils.data import DataLoader, TensorDataset
import torch
# noinspection PyUnresolvedReferences
from torch import from_numpy
import numpy
from torch.nn import DataParallel
from logging import getLogger

logger = getLogger(__name__)


class Model(model.Model):
    weights_serializer_class = 'tatau_core.nn.torch.serializer.WeightsSerializer'
    weights_summarizer_class = 'tatau_core.nn.torch.summarizer.Mean'

    def __init__(self, optimizer_class, optimizer_kwargs, criterion):
        super(Model, self).__init__()

        self._optimizer_kwargs = optimizer_kwargs

        self._device = 'cuda' if torch.cuda.is_available() else 'cpu'

        logger.info("Model device: {}".format(self.device))

        self._model = self.native_model_factory()

        if torch.cuda.is_available() and torch.cuda.device_count() > 1:
            logger.info("Gpu count: {}".format(torch.cuda.device_count()))
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

    def train(self, x: numpy.array, y: numpy.array, batch_size: int, current_iteration: int,
              nb_epochs: int, train_progress: TrainProgress):

        self.native_model.train()

        x, y = self.data_preprocessing(x, y)

        dataset = TensorDataset(from_numpy(x), from_numpy(y))
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)

        train_history = {'loss': [], 'acc': []}
        for epoch in range(1, nb_epochs + 1):
            epoch_loss = 0.0
            # running_loss = 0.0
            correct = 0
            for batch_idx, (input_, target) in enumerate(loader, 0):
                input_, target = input_.to(self.device), target.to(self.device)
                self.optimizer.zero_grad()
                output = self.native_model(input_)
                loss = self._criterion(output, target)
                epoch_loss += loss.item()
                _, predicted = torch.max(output.data, 1)
                correct += predicted.eq(target).sum().item()
                loss.backward()
                self.optimizer.step()
                # running_loss += loss.item()

                # if batch_idx >0 and batch_idx % 200 == 0:
                #     print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                #         epoch, batch_idx * len(data), len(loader.dataset),
                #                100. * batch_idx / len(loader), loss.item()))
                #     running_loss = 0.0
            epoch_loss = epoch_loss / len(loader)
            epoch_acc = correct/len(loader.dataset)
            logger.info("Epoch #{}: Loss: {:.4f} Acc: {:.2f}".format(epoch, epoch_loss, 100 * epoch_acc))
            train_history['loss'].append(epoch_loss)
            train_history['acc'].append(epoch_acc)
        return train_history

    def eval(self, x: numpy.array, y: numpy.array):
        # noinspection PyUnresolvedReferences
        from torch import from_numpy
        self.native_model.eval()
        test_loss = 0
        correct = 0
        dataset = TensorDataset(from_numpy(x), from_numpy(y))
        loader = DataLoader(dataset, batch_size=128, shuffle=False, num_workers=0)

        with torch.no_grad():
            for input_, target in loader:
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
