from tatau_core.nn.tatau import model
from tatau_core.nn.tatau import TrainProgress
from torch.utils.data import DataLoader, TensorDataset
import torch
# noinspection PyUnresolvedReferences
from torch import from_numpy
import numpy


class Model(model.Model):
    weights_serializer_class = 'tatau_core.nn.torch.serializer.WeightsSerializer'
    weights_summarizer_class = 'tatau_core.nn.torch.summarizer.Median'

    def __init__(self, optimizer_class, optimizer_kwargs, criterion):
        super(Model, self).__init__()
        self._optimizer_class = optimizer_class
        self._optimizer_kwargs = optimizer_kwargs
        self._criterion = criterion
        self._optimizer_class = optimizer_class
        self._optimizer_instance = None

    @property
    def optimizer(self):
        if not self._optimizer_instance:
            self._optimizer_instance = self._optimizer_class(self.native_model.parameters(), **self._optimizer_kwargs)
        return self._optimizer_instance

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

    def train(self, x: numpy.array, y: numpy.array, batch_size: int, nb_epochs: int, train_progress: TrainProgress):

        self.native_model.train()

        dataset = TensorDataset(from_numpy(x), from_numpy(y))
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)

        train_history = {'loss': [], 'acc': []}
        for epoch in range(1, nb_epochs + 1):
            epoch_loss = 0.0
            # running_loss = 0.0
            correct = 0
            for batch_idx, (data, target) in enumerate(loader, 0):
                # TODO move on device
                # data, target = data.to(device), target.to(device)
                self.optimizer.zero_grad()
                output = self.native_model(data)
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
            print("Epoch #{}: Loss: {:.4f} Acc: {:.2f}".format(epoch, epoch_loss, 100 * epoch_acc))
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
            for data in loader:
                # TODO: check device compatibility
                images, labels = data
                outputs = self.native_model(images)
                loss = self._criterion(outputs, labels)
                test_loss += loss.item()
                # noinspection PyUnresolvedReferences
                _, predicted = torch.max(outputs.data, 1)
                correct += predicted.eq(labels).sum().item()
                # correct += (predicted == target).sum().item()

        test_loss /= len(loader)
        test_acc = correct / len(loader.dataset)
        print('\nTest set: Average loss: {:.8f}, Accuracy: {}/{} ({:.4f}%)\n'.format(
            test_loss, correct, len(loader.dataset), test_acc))
        return test_loss, test_acc
