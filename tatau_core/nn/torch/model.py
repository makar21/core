import numpy
from tatau_core.nn import tatau
from tatau_core.nn.tatau import TrainProgress
import torch
from torch.utils.data import DataLoader, TensorDataset
# noinspection PyUnresolvedReferences
from torch import from_numpy


class Model(tatau.Model):
    def __init__(self, optimizer_class, optimizer_kwargs, criterion):
        super(Model, self).__init__()
        self._optimizer_class = optimizer_class
        self._optimizer_kwargs = optimizer_kwargs
        self._criterion = criterion
        self._optimizer = optimizer_class(self.native_model.parameters(), **optimizer_kwargs)

    @classmethod
    def native_model_factory(cls) -> torch.nn.Module:
        raise NotImplementedError()

    def get_weights(self):
        pass

    def set_weights(self, weights: list):
        pass

    def train(self, x: numpy.array, y: numpy.array, batch_size: int, nb_epochs: int, train_progress: TrainProgress):
        self.native_model.train()
        dataset = TensorDataset(from_numpy(x), from_numpy(y))
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=False, num_workers=0)

        train_history = {'loss': [], 'acc': []}
        for epoch in range(1, nb_epochs + 1):
            epoch_loss = 0.0
            # running_loss = 0.0
            correct = 0
            for batch_idx, (data, target) in enumerate(loader, 0):
                # TODO move on device
                # data, target = data.to(device), target.to(device)
                self._optimizer.zero_grad()
                output = self.native_model(data)
                loss = self._criterion(output, target)
                epoch_loss += loss.item()
                _, predicted = torch.max(output.data, 1)
                correct += predicted.eq(target).sum().item()
                loss.backward()
                self._optimizer.step()
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
