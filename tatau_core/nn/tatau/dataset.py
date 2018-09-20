import numpy as np
from torch.utils.data import Dataset as TorchDataset


class Dataset(TorchDataset):
    def __init__(self, x_path, y_path, transform=None):
        self.x = self.load_file(x_path)
        self.y = self.load_file(y_path)
        assert len(self.x) == len(self.y)
        self.transform = transform

    @classmethod
    def load_file(cls, path):
        data = np.load(path, mmap_mode='r')
        if issubclass(np.lib.npyio.NpzFile, data.__class__):
            with data:
                return data['arr_0']
        elif isinstance(data, np.core.memmap):
            return data

        raise RuntimeError("Unsupported numpy format")

    def __getitem__(self, index):
        x = self.x[index] if self.transform else self.transform(self.x[index])
        return x, self.y[index]

    def __len__(self):
        return len(self.x)
