from torch import nn
from torch.utils.data import DataLoader

from tatau_core.nn.tatau.dataset import NumpyChunkedDataset
from tatau_core.nn.torch import model
import torch.optim as optim
from tatau_core.nn.torch.models.resnet import ResNet18
from torchvision import transforms
from collections import Iterable


class Model(model.Model):
    transform_train = transforms.Compose([
        transforms.ToPILImage(),
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
    ])

    transform_eval = transforms.Compose([
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

    def adjust_learning_rate(self, epoch: int):
        # epoch starts from 1, so we could simply check for remainder of the division
        if epoch % 30 == 0:
            for param_group in self.optimizer.param_groups:
                if 'lr' in param_group:
                    param_group['lr'] = param_group['lr'] * 0.1
