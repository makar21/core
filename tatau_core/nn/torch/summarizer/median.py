from tatau_core.nn.tatau.summarizer import Summarizer
import numpy as np
from .opt import MedianOptimizerSummarizer
from .weights import MedianWeightsSummarizer


class Median(Summarizer):
    """
    Median State Summarizer
    """

    def summarize(self, updates):
        if not len(updates):
            raise RuntimeError("No updates")

        optimizer_summarizer = MedianOptimizerSummarizer()
        weights_summarizer = MedianWeightsSummarizer()

        for state in updates:
            optimizer_summarizer.update(state['optimizer'])
            weights_summarizer.update(state['weights'])

        new_state = {
            'optimizer': optimizer_summarizer.commit(),
            'weights': weights_summarizer.commit()
        }
        return new_state
