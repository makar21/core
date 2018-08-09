from collections import deque

from tatau_core.nn.tatau import serializer


class WeightsSerializer(serializer.WeightsSerializer):
    @classmethod
    def save(cls, weights: dict, path: str):
        """
        Save model state
        :param weights: state
        :param path: state file path
        :return: None
        """
        # Lazy load torch
        from torch import save as torch_save
        torch_save(weights, path)

    @classmethod
    def load(cls, path: str):
        """
        Load model state
        :param path: state file path
        :return: state
        """
        # Lazy load torch
        from torch import load as torch_load
        return torch_load(path)

    @classmethod
    def to_numpy(cls, weights):
        from torch import is_tensor
        layers = deque()
        state_dict = weights['weights']
        for key in sorted(state_dict.keys()):
            value = state_dict[key]
            if is_tensor(value):
                layers.append(value.detach().numpy())

        return layers
