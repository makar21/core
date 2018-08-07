from abc import ABC, abstractmethod


class Callback(ABC):
    @abstractmethod
    def callback(self, *args, **kwargs):
        pass
