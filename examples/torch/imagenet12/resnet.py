import torchvision.models as models
import torch.optim as optim
from torch import nn
from torchvision import transforms
from tatau_core.nn.torch import model
from torch.utils.data import Dataset, ConcatDataset
from collections import Iterable
from tatau_core.nn.torch.data_loader import NumpyDataChunk


class Model(model.Model):
    transform = transforms.Compose([transforms.ToPILImage(),
                                    transforms.RandomCrop(224, padding=4),
                                    transforms.RandomHorizontalFlip(),
                                    transforms.ToTensor(),
                                    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

    @classmethod
    def native_model_factory(cls) -> nn.Module:
        return models.resnet18()

    def data_preprocessing(self, x_path_list: Iterable, y_path_list: Iterable) -> Dataset:
        chunks = [NumpyDataChunk(x_path, y_path, transform=self.transform)
                  for x_path, y_path in zip(x_path_list, y_path_list)]
        return ConcatDataset(chunks)

    def __init__(self):
        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.1, momentum=0.9, weight_decay=1e-4),
            criterion=nn.CrossEntropyLoss()
        )
