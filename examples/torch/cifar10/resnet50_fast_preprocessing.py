from collections import Iterable
import torch.optim as optim
from torch import nn
from torchvision import transforms
from torch.utils.data import DataLoader

from tatau_core.nn.tatau.dataset import NumpyChunkedDataset
from tatau_core.nn.torch import model
from tatau_core.nn.torch.models.resnet import ResNet50
from tatau_core.nn.torch.utils.fast_preprocessing import fast_collate, DataPrefetcher


class Model(model.Model):
    transform_train = transforms.Compose([
        transforms.ToPILImage(),
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
    ])

    @classmethod
    def native_model_factory(cls) -> nn.Module:
        return ResNet50(num_classes=10)

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

    def data_preprocessing(self, chunk_dirs: Iterable, batch_size, transform: callable) -> Iterable:
        data_loader = DataLoader(
                    dataset=NumpyChunkedDataset(chunk_dirs=chunk_dirs, transform=transform),
                    batch_size=batch_size,
                    shuffle=True,
                    pin_memory=False,
                    collate_fn=fast_collate)
        return DataPrefetcher(data_loader,
                              normalize_mean=[0.4914 * 255, 0.4822 * 255, 0.4465 * 255],
                              normalize_std=[0.2023 * 255, 0.1994 * 255, 0.2010 * 255])
