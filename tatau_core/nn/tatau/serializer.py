class WeightsSerializer:
    @classmethod
    def save(cls, weights, path):
        raise NotImplementedError()

    @classmethod
    def load(cls, path):
        raise NotImplementedError()
