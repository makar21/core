import abc
import os
import pickle
from logging import getLogger

import numpy as np
from torch.utils.data import Dataset as TorchDataset, ConcatDataset, DataLoader
from torchvision.datasets.folder import default_loader, find_classes, make_dataset, IMG_EXTENSIONS

logger = getLogger(__name__)


class ChunkedDataset(TorchDataset):
    def __init__(self, chunk_dir, transform=None):
        self._transform = transform
        self._x, self._y = self.open_chunk(chunk_dir)

    @abc.abstractmethod
    def open_chunk(self, chunk_dir):
        pass


class NumpyDataset(ChunkedDataset):
    def __init__(self, chunk_dir, mmap_mode='r', transform=None):
        self._mmap_mode = mmap_mode
        super(NumpyDataset, self).__init__(chunk_dir=chunk_dir, transform=transform)

    def open_chunk(self, chunk_dir):
        x = np.load(os.path.join(chunk_dir, "x.npy"), mmap_mode=self._mmap_mode)
        y = np.load(os.path.join(chunk_dir, "y.npy"), mmap_mode=self._mmap_mode)
        return x, y

    def __getitem__(self, index):
        x = self._x[index] if self._transform is None else self._transform(self._x[index])
        return x, self._y[index]

    def __len__(self):
        return len(self._x)


class NumpyChunkedDataset(ConcatDataset):
    def __init__(self, chunk_dirs, mmap_mode=None, transform=None):
        super(NumpyChunkedDataset, self).__init__(
            datasets=[
                NumpyDataset(chunk_dir=chunk_dir, mmap_mode=mmap_mode, transform=transform)
                for chunk_dir in chunk_dirs
            ]
        )


class CachedFolderDataset(TorchDataset):
    """A generic data loader where the samples are arranged in this way: ::

        root/class_x/xxx.ext
        root/class_x/xxy.ext
        root/class_x/xxz.ext

        root/class_y/123.ext
        root/class_y/nsdf3.ext
        root/class_y/asd932_.ext

    Args:
        root (string): Root directory path.
        loader (callable): A function to load a sample given its path.
        extensions (list[string]): A list of allowed extensions.
        transform (callable, optional): A function/transform that takes in
            a sample and returns a transformed version.
            E.g, ``transforms.RandomCrop`` for images.
        target_transform (callable, optional): A function/transform that takes
            in the target and transforms it.

     Attributes:
        classes (list): List of the class names.
        class_to_idx (dict): Dict with items (class_name, class_index).
        samples (list): List of (sample path, class_index) tuples
    """

    def __init__(self, root, loader, extensions, transform=None, target_transform=None):
        chunk_name = os.path.basename(root)
        chunk_dir = os.path.dirname(root)
        self._cache_path = os.path.join(chunk_dir, "{}.cache".format(chunk_name))

        if not os.path.exists(self._cache_path):
            logger.info("Create dataset for dir: {}".format(root))
            classes, class_to_idx = find_classes(root)
            samples = make_dataset(root, class_to_idx, extensions)
            if len(samples) == 0:
                raise (RuntimeError("Found 0 files in subfolders of: " + root + "\n"
                                                                                "Supported extensions are: " + ",".join(
                    extensions)))

            logger.info("Save cache to: {}".format(self._cache_path))
            with open(self._cache_path, 'wb') as cache_file:
                pickle.dump(
                    dict(classes=classes, class_to_idx=class_to_idx, samples=samples),
                    cache_file,
                    pickle.HIGHEST_PROTOCOL
                )
        else:
            logger.info("Load cache from: {}".format(self._cache_path))
            with open(self._cache_path, 'rb') as cache_file:
                data = pickle.load(cache_file)
                classes = data['classes']
                class_to_idx = data['class_to_idx']
                samples = data['samples']

        logger.info("Dataset samples: {}".format(len(samples)))

        self.root = root
        self.loader = loader
        self.extensions = extensions

        self.classes = classes
        self.class_to_idx = class_to_idx
        self.samples = samples

        self.transform = transform
        self.target_transform = target_transform

    def __getitem__(self, index):
        """
        Args:
            index (int): Index

        Returns:
            tuple: (sample, target) where target is class_index of the target class.
        """
        path, target = self.samples[index]
        sample = self.loader(path)
        if self.transform is not None:
            sample = self.transform(sample)
        if self.target_transform is not None:
            target = self.target_transform(target)

        return sample, target

    def __len__(self):
        return len(self.samples)

    def __repr__(self):
        fmt_str = 'Dataset ' + self.__class__.__name__ + '\n'
        fmt_str += '    Number of datapoints: {}\n'.format(self.__len__())
        fmt_str += '    Root Location: {}\n'.format(self.root)
        tmp = '    Transforms (if any): '
        fmt_str += '{0}{1}\n'.format(tmp, self.transform.__repr__().replace('\n', '\n' + ' ' * len(tmp)))
        tmp = '    Target Transforms (if any): '
        fmt_str += '{0}{1}'.format(tmp, self.target_transform.__repr__().replace('\n', '\n' + ' ' * len(tmp)))
        return fmt_str


class CachedImageFolder(CachedFolderDataset):
    def __init__(self, root, transform=None, target_transform=None, loader=default_loader):
        """A generic data loader where the images are arranged in this way: ::

            root/dog/xxx.png
            root/dog/xxy.png
            root/dog/xxz.png

            root/cat/123.png
            root/cat/nsdf3.png
            root/cat/asd932_.png

        Args:
            root (string): Root directory path.
            transform (callable, optional): A function/transform that  takes in an PIL image
                and returns a transformed version. E.g, ``transforms.RandomCrop``
            target_transform (callable, optional): A function/transform that takes in the
                target and transforms it.
            loader (callable, optional): A function to load an image given its path.

         Attributes:
            classes (list): List of the class names.
            class_to_idx (dict): Dict with items (class_name, class_index).
            imgs (list): List of (image path, class_index) tuples
        """
        super(CachedImageFolder, self).__init__(root, loader, IMG_EXTENSIONS,
                                                transform=transform,
                                                target_transform=target_transform)
        self.imgs = self.samples


class AutoDataLoader(DataLoader):
    """
    Basic data loader with automatic tune num_workers
    """
    def __init__(self, *args, **kwargs):
        kwargs['num_workers'] = os.cpu_count()
        logger.info("Data Loader use {} workers".format(kwargs['num_workers']))
        super(AutoDataLoader, self).__init__(*args, **kwargs)
