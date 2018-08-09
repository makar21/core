from tatau_core.nn.tatau.summarizer import Summarizer
from collections import deque
import numpy as np


class OptimizerSummarizer(Summarizer):
    def __init__(self, np_sum_fn):
        super(OptimizerSummarizer, self).__init__()
        self._np_sum_fn = np_sum_fn

    def summarize(self, updates):
        import torch

        states = dict()
        params = dict()

        for u in updates:
            assert len(u['param_groups']) == 1

            param_groups = u['param_groups'][0]

            for num, item_key in enumerate(param_groups['params']):
                k, v = item_key, u['state'][item_key]
                if num not in states:
                    states[num] = dict()

                for kk, vv in v.items():
                    if kk not in states[num]:
                        states[num][kk] = deque()
                    states[num][kk].append(vv.detach().numpy())

            for name, value in param_groups.items():
                if name == 'params':
                    continue
                if name not in params:
                    params[name] = deque()
                params[name].append(value)

        for k, v in params.items():
            params[k] = self._np_sum_fn(np.asarray(v), axis=0)

        for state_num, state_dict in states.items():
            for name, value in state_dict.items():
                arr_values = np.asarray(value)
                state_dict[name] = torch.from_numpy(self._np_sum_fn(arr_values, axis=0))

        params['params'] = list(states.keys())

        return dict(
            state=states,
            param_groups=[params]
        )


class MedianOptimizerSummarizer(OptimizerSummarizer):
    def __init__(self):
        super(MedianOptimizerSummarizer, self).__init__(np_sum_fn=np.median)


class MeanOptimizerSummarizer(OptimizerSummarizer):
    def __init__(self):
        super(MeanOptimizerSummarizer, self).__init__(np_sum_fn=np.mean)
