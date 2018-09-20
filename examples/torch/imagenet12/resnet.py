import numpy as np
import torchvision.models as models
import torch.optim as optim
from torch import nn
from torchvision import transforms
from tatau_core.nn.torch import model


class Model(model.Model):
    transforms_train = transforms.Compose([
        transforms.Lambda(lambda x: np.transpose(x)),
        transforms.ToPILImage(),
        transforms.RandomCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

    transforms_eval = transforms.Compose([
        transforms.Lambda(lambda x: np.transpose(x)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

    @classmethod
    def native_model_factory(cls) -> nn.Module:
        return models.resnet18()

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

