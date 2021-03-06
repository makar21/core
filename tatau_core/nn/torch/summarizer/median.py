from .model import ModelSummarizer
import numpy as np


class Median(ModelSummarizer):
    """
    Median State Summarizer
    """
    def __init__(self):
        super(Median, self).__init__(np_sum_fn=np.median)
