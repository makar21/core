from .model import ModelSummarizer
import numpy as np


class Mean(ModelSummarizer):
    """
    Median State Summarizer
    """
    def __init__(self):
        super(Mean, self).__init__(np_sum_fn=np.mean)