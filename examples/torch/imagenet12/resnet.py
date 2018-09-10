import torchvision.models as models
import torch.optim as optim
from torch import nn
from tatau_core.nn.torch import model


class Model(model.Model):

    @classmethod
    def native_model_factory(cls) -> nn.Module:
        return models.resnet18()

    def __init__(self):
        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.1, momentum=0.9, weight_decay=1e-4),
            criterion=nn.CrossEntropyLoss()
        )
