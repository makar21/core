import abc
import numpy as np
from torch.utils.data import Dataset as TorchDataset
import os


class ChunkedDataset(TorchDataset):
    def __init__(self, chunk_dir, transform=None):
        self._transform = transform
        self._x, self._y = self.open_chunk(chunk_dir)

    @abc.abstractmethod
    def open_chunk(self, chunk_dir):
        pass


class NumpyChunkedDataset(ChunkedDataset):
    def __init__(self, chunk_dir, mmap_mode='r', transform=None):
        self._mmap_mode = mmap_mode
        super(NumpyChunkedDataset, self).__init__(chunk_dir=chunk_dir, transform=transform)

    def open_chunk(self, chunk_dir):
        x = np.load(os.path.join(chunk_dir, "x.npy"), mmap_mode=self._mmap_mode)
        y = np.load(os.path.join(chunk_dir, "y.npy"), mmap_mode=self._mmap_mode)
        return x, y

    def __getitem__(self, index):
        x = self._x[index] if self._transform is None else self._transform(self._x[index])
        return x, self._y[index]

    def __len__(self):
        return len(self._x)
