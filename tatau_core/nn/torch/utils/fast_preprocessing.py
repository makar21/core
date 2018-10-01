import torch
import numpy as np


def fast_collate(batch):
    images = [img[0] for img in batch]
    # noinspection PyCallingNonCallable,PyUnresolvedReferences
    targets = torch.tensor([target[1] for target in batch], dtype=torch.int64)
    w = images[0].size[0]
    h = images[0].size[1]
    # noinspection PyUnresolvedReferences
    tensor = torch.zeros((len(images), 3, h, w), dtype=torch.uint8)
    for i, img in enumerate(images):
        nump_array = np.asarray(img, dtype=np.uint8)
        if nump_array.ndim < 3:
            nump_array = np.expand_dims(nump_array, axis=-1)
        nump_array = np.rollaxis(nump_array, 2)

        # noinspection PyUnresolvedReferences
        tensor[i] += torch.from_numpy(nump_array)

    return tensor, targets


class DataPrefetcher:
    def __init__(self, loader, normalize_mean=None, normalize_std=None, is_fp16=False):
        self._loader = loader
        self._loader_iter = iter(loader)
        self.dataset = loader.dataset
        self._stream = torch.cuda.Stream()
        self._mean = None
        self._std = None
        self._next_target = None
        self._next_input = None
        self._is_fp16 = is_fp16
        if normalize_mean is not None:
            # noinspection PyCallingNonCallable
            self._mean = torch.tensor(normalize_mean).cuda().view(1, 3, 1, 1)
            if self._is_fp16:
                self._mean = self._mean.half()
        if normalize_std is not None:
            # noinspection PyCallingNonCallable
            self._std = torch.tensor(normalize_std).cuda().view(1, 3, 1, 1)
            if self._is_fp16:
                self._std = self._std.half()

        self._preload()

    def _preload(self):
        try:
            self._next_input, self._next_target = next(self._loader_iter)
        except StopIteration:
            self._next_input = None
            self._next_target = None
            return
        with torch.cuda.stream(self._stream):
            self._next_input = self._next_input.cuda(async=True)
            self._next_target = self._next_target.cuda(async=True)

            if self._fp16:
                self._next_target = self._next_target.half()
            else:
                self._next_input = self._next_input.float()

            if self._mean is not None:
                self._next_input = self._next_input.sub_(self._mean)
            if self._std is not None:
                self._next_input = self._next_input.div_(self._std)

    def __next__(self):
        torch.cuda.current_stream().wait_stream(self._stream)
        input_ = self._next_input
        target = self._next_target
        if input_ is None:
            self._loader_iter = iter(self._loader)
            self._preload()
            raise StopIteration
        self._preload()
        return input_, target

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._loader)
