from abc import ABC


class WeightsSerializer(ABC):
    @classmethod
    def save(cls, weights, path):
        raise NotImplementedError()

    @classmethod
    def load(cls, path):
        raise NotImplementedError()

    @classmethod
    def to_numpy(cls, weights):
        raise NotImplementedError()
