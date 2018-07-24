from .summarizer import Summarizer
import numpy as np


class Median(Summarizer):
    """
    Median Weights Summarizer
    """

    def summarize(self, updates):
        if not len(updates):
            raise RuntimeError("No updates")

        new_weights = list()
        for weights_list_tuple in zip(*updates):
            new_weights.append(
                [
                    np.median(a=np.array(weights_, dtype=self.dtype), axis=0)
                    for weights_ in zip(*weights_list_tuple)
                ]
            )
        return new_weights
