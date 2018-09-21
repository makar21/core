import torch
from torch import nn
from tatau_core.nn.torch import model
import torch.optim as optim
from tatau_core.nn.torch.models.resnet import ResNet18
from torchvision import transforms

from tatau_core.nn.tatau import TrainProgress
from torch.utils.data import DataLoader
from logging import getLogger

from collections import Iterable

from math import floor

logger = getLogger(__name__)


# Cyclic learning rate
def cycle(iteration, stepsize):
    return floor(1 + iteration / (2 * stepsize))


def abs_pos(cycle_num, iteration, stepsize):
    return abs(iteration / stepsize - 2 * cycle_num + 1)


def rel_pos(iteration, stepsize):
    return max(0, (1-abs_pos(cycle(iteration, stepsize), iteration, stepsize)))


def cyclic_learning_rate(min_lr, max_lr, stepsize):
    return lambda iteration: min_lr + (max_lr - min_lr) * rel_pos(iteration, stepsize)


class Model(model.Model):
    transforms_train = transforms.Compose([
        transforms.ToPILImage(),
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])

    transforms_eval = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])

    @classmethod
    def native_model_factory(cls) -> nn.Module:
        return ResNet18(num_classes=10)

    def __init__(self):
        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.1, momentum=0.9, weight_decay=1e-4),
            criterion=nn.CrossEntropyLoss()
        )
        self.scheduler = None
        self.initialize_clr_scheduler(0.01, 0.1, 5)

    def initialize_clr_scheduler(self, min_lr, max_lr, stepsize):
        clr_lambda = cyclic_learning_rate(min_lr, max_lr, stepsize)
        self.optimizer.lr = 1
        self.scheduler = optim.lr_scheduler.LambdaLR(self.optimizer, lr_lambda=[clr_lambda])

    def train(self, x_path_list: Iterable, y_path_list: Iterable, batch_size: int, current_iteration: int,
              nb_epochs: int, train_progress: TrainProgress):
        self.native_model.train()
        dataset = self.data_preprocessing(x_path_list, y_path_list, transforms=self.transforms_train)
        batch_size = batch_size * max(1, self._gpu_count)
        logger.info("Batch size: {}".format(batch_size))
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=False, num_workers=0, pin_memory=False)

        train_history = {'loss': [], 'acc': []}
        for epoch in range(1, nb_epochs + 1):
            if self.scheduler is not None:
                self.scheduler.step()
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
            epoch_acc = correct / len(loader.dataset)
            logger.info("Epoch #{}: Loss: {:.4f} Acc: {:.2f}".format(epoch, epoch_loss, 100 * epoch_acc))
            train_history['loss'].append(epoch_loss)
            train_history['acc'].append(epoch_acc)
        return train_history

    def adjust_learning_rate(self, epoch: int):
        pass
