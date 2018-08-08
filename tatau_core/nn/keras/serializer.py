from tatau_core.nn.tatau import serializer
import numpy as np
from os import rename


class WeightsSerializer(serializer.WeightsSerializer):
    @classmethod
    def save(cls, weights, path):
        path_npz = "{}.npz".format(path)
        np.savez_compressed(path_npz, *weights)
        rename(path_npz, path)

    @classmethod
    def load(cls, path):
        path_npz = "{}.npz".format(path)
        weights_file = np.loads(path_npz)
        return [weights_file[r] for r in weights_file.files]
