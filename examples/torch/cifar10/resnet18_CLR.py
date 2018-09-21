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
        self.initialize_clr_scheduler(0.1, 1., 5)

    def initialize_clr_scheduler(self, min_lr, max_lr, stepsize):
        clr_lambda = cyclic_learning_rate(min_lr, max_lr, stepsize)
        self.scheduler = optim.lr_scheduler.LambdaLR(self.optimizer, lr_lambda=[clr_lambda])

    def adjust_learning_rate(self, epoch: int):
        self.scheduler.step(epoch-1)
