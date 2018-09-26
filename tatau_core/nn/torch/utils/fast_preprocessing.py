import torch
import numpy as np


def fast_collate(batch):
    imgs = [img[0] for img in batch]
    targets = torch.tensor([target[1] for target in batch], dtype=torch.int64)
    w = imgs[0].size[0]
    h = imgs[0].size[1]
    tensor = torch.zeros( (len(imgs), 3, h, w), dtype=torch.uint8 )
    for i, img in enumerate(imgs):
        nump_array = np.asarray(img, dtype=np.uint8)
        if(nump_array.ndim < 3):
            nump_array = np.expand_dims(nump_array, axis=-1)
        nump_array = np.rollaxis(nump_array, 2)

        tensor[i] += torch.from_numpy(nump_array)

    return tensor, targets


class DataPrefetcher():
    def __init__(self, loader, normalize_mean=None, normalize_std=None):
        self.loader = iter(loader)
        self.stream = torch.cuda.Stream()
        self.mean = None
        self.std = None
        if normalize_mean is not None:
            self.mean = torch.tensor(normalize_mean).cuda().view(1,3,1,1)
        if normalize_std is not None:
            self.std = torch.tensor(normalize_std).cuda().view(1,3,1,1)
        self.preload()

    def preload(self):
        try:
            self.next_input, self.next_target = next(self.loader)
        except StopIteration:
            self.next_input = None
            self.next_target = None
            return
        with torch.cuda.stream(self.stream):
            self.next_input = self.next_input.cuda(async=True)
            self.next_target = self.next_target.cuda(async=True)
            self.next_input = self.next_input.float()
            if self.mean is not None:
                self.next_input = self.next_input.sub_(self.mean)
            if self.std is not None:
                self.next_input = self.next_input.div_(self.std)

    def next(self):
        torch.cuda.current_stream().wait_stream(self.stream)
        input = self.next_input
        target = self.next_target
        self.preload()
        return input, target

    def __iter__(self):
        return self.loader

    def __len__(self):
        return len(self.loader)
