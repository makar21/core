# noinspection PyPep8Naming
import torch.nn.functional as F
import torch.optim as optim
from torch.nn.modules import Module, Conv2d, Linear, MaxPool2d, CrossEntropyLoss
from torchvision import transforms

from tatau_core.nn.torch import model


class Net(Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = Conv2d(3, 6, 5)
        self.pool = MaxPool2d(2, 2)
        self.conv2 = Conv2d(6, 16, 5)
        self.fc1 = Linear(16 * 5 * 5, 120)
        self.fc2 = Linear(120, 84)
        self.fc3 = Linear(84, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = x.view(-1, 16 * 5 * 5)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


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
    def native_model_factory(cls) -> Module:
        return Net()

    def __init__(self):
        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.001, momentum=0.9),
            criterion=CrossEntropyLoss()
        )

    def adjust_learning_rate(self, epoch: int):
        # epoch starts from 1, so we could simply check for remainder of the division
        if epoch % 30 == 0:
            for param_group in self.optimizer.param_groups:
                if 'lr' in param_group:
                    param_group['lr'] = param_group['lr'] * 0.1
