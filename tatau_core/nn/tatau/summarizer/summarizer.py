from collections import deque
import numpy as np
from abc import abstractmethod, ABC


class Summarizer(ABC):
    """
    Basic Summarizer
    """
    dtype = np.float32

    def __init__(self):
        self._updates = deque()

    def update(self, weights):
        self._updates.append(weights)

    @abstractmethod
    def summarize(self, updates):
        pass

    def commit(self):
        weights = self.summarize(self._updates)
        self._updates.clear()
        return weights
