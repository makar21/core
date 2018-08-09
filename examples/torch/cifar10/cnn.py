from tatau_core.nn.torch import model
from torch.nn.modules import Module, Conv2d, Linear, MaxPool2d, CrossEntropyLoss
# noinspection PyPep8Naming
import torch.nn.functional as F
import torch.optim as optim


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

    @classmethod
    def native_model_factory(cls) -> Module:
        return Net()

    def __init__(self):
        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.001, momentum=0.9),
            criterion=CrossEntropyLoss()
        )

