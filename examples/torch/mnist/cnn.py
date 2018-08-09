from tatau_core.nn.torch import model
from torch.nn.modules import Module, Conv2d, Linear, Dropout2d, CrossEntropyLoss
# noinspection PyPep8Naming
import torch.nn.functional as F
import torch.optim as optim


class Net(Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = Conv2d(1, 10, kernel_size=5)
        self.conv2 = Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = Dropout2d()
        self.fc1 = Linear(320, 50)
        self.fc2 = Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


class Model(model.Model):

    @classmethod
    def native_model_factory(cls) -> Module:
        return Net()

    def __init__(self):
        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.01, momentum=0.5),
            criterion=CrossEntropyLoss()
        )

