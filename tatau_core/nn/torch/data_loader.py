import torch
import numpy as np
from torch.utils.data import Dataset


class NumpyDataChunk(Dataset):
    def __init__(self, x_path, y_path, transform=None):
        with np.load(x_path, mmap_mode='r') as data:
            self.x = data['arr_0']  # x.shape = (chunk_size, C, H, W)
        with np.load(y_path, mmap_mode='r') as data:
            self.y = data['arr_0']  # y.shape = (chunk_size,)
        assert len(self.x) == len(self.y)
        self.transform = transform

    def __getitem__(self, index):
        if self.transform:
            x_normed = self.transform(np.transpose(self.x[index]))
        else:
            x_normed = torch.from_numpy(self.x[index])
        x_normed = x_normed.type(torch.FloatTensor)
        return x_normed, self.y[index]

    def __len__(self):
        return len(self.x)
