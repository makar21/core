from tatau_core.nn.tatau.summarizer import Summarizer
from .opt import OptimizerSummarizer
from .weights import WeightsSummarizer


class ModelSummarizer(Summarizer):
    """
    Median State Summarizer
    """
    def __init__(self, np_sum_fn):
        super(ModelSummarizer, self).__init__()
        self._np_sum_fn = np_sum_fn

    def summarize(self, updates):
        if not len(updates):
            raise RuntimeError("No updates")

        optimizer_summarizer = OptimizerSummarizer(np_sum_fn=self._np_sum_fn)
        weights_summarizer = WeightsSummarizer(np_sum_fn=self._np_sum_fn)

        for state in updates:
            optimizer_summarizer.update(state['optimizer'])
            weights_summarizer.update(state['weights'])

        new_state = {
            'optimizer': optimizer_summarizer.commit(),
            'weights': weights_summarizer.commit()
        }
        return new_state

