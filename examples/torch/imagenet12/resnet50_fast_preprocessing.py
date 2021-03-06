from collections import Iterable
from logging import getLogger

import torch
import torch.optim as optim
from torch.nn import Module, Linear, Parameter, CrossEntropyLoss
from torch.utils.data import DataLoader, ConcatDataset
from torchvision import transforms
from torchvision.datasets.folder import ImageFolder
from torchvision.models.resnet import resnet50, BasicBlock, Bottleneck

from tatau_core.nn.torch import model
from tatau_core.nn.torch.utils.fast_preprocessing import fast_collate, DataPrefetcher

logger = getLogger('tatau_core')


class Model(model.Model):
    transform_train = transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip()
        ])

    transform_eval = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224)
    ])

    @classmethod
    def native_model_factory(cls) -> Module:
        model_ = resnet50()
        # https://arxiv.org/pdf/1706.02677.pdf
        # https://github.com/pytorch/examples/pull/262
        # for m in model_.modules():
        #     if isinstance(m, BasicBlock): m.bn2.weight = Parameter(torch.zeros_like(m.bn2.weight))
        #     if isinstance(m, Bottleneck): m.bn3.weight = Parameter(torch.zeros_like(m.bn3.weight))
        #     if isinstance(m, Linear): m.weight.data.normal_(0, 0.01)
        return model_

    def __init__(self):

        super(Model, self).__init__(
            optimizer_class=optim.SGD,
            optimizer_kwargs=dict(lr=0.1, momentum=0.9, weight_decay=1e-4),
            criterion=CrossEntropyLoss()
        )

    def adjust_learning_rate(self, epoch: int):
        epoch = epoch - 1
        """LR schedule that should yield 76% converged accuracy with batch size 256"""
        factor = epoch // 50

        if epoch >= 80:
            factor = factor + 1

        lr = 0.1 * (0.1 ** factor)

        """Warmup"""
        # if epoch < 5:
        #     lr = lr * float(1 + step + epoch * len_epoch) / (5. * len_epoch)

        # if(args.local_rank == 0):
        #     print("epoch = {}, step = {}, lr = {}".format(epoch, step, lr))

        for param_group in self.optimizer.param_groups:
            param_group['lr'] = lr

    def data_preprocessing(self, chunk_dirs: Iterable, batch_size, transform: callable) -> Iterable:
        data_loader = DataLoader(
            dataset=ConcatDataset([ImageFolder(root=chunk_dir, transform=transform) for chunk_dir in chunk_dirs]),
            batch_size=batch_size,
            shuffle=True,
            pin_memory=False,
            num_workers=10,
            collate_fn=fast_collate)
        return DataPrefetcher(data_loader,
                              normalize_mean=[0.485 * 255, 0.456 * 255, 0.406 * 255],
                              normalize_std=[0.229 * 255, 0.224 * 255, 0.225 * 255]
                              )
