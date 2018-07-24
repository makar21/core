from collections import deque
import numpy as np


class Summarizer(object):
    """
    Basic Weights Summarizer
    """
    dtype = np.float32

    def __init__(self):
        self._updates = deque()

    def update(self, weights):
        self._updates.append(weights)

    def summarize(self, updates):
        raise NotImplementedError()

    def commit(self):
        weights = self.summarize(self._updates)
        self._updates.clear()
        return weights
