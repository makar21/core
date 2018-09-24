import argparse
from logging import getLogger

from tatau_core.nn.benchmark.run import benchmark_train
from tatau_core.utils.logging import configure_logging

configure_logging(__name__)

logger = getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Local benchmark')

    parser.add_argument('-m', '--model_path', required=True, help='model path')
    parser.add_argument('-train', '--dataset_train', required=True, help='path to dir with train dataset chunks')
    parser.add_argument('-test', '--dataset_test', required=True, help='path to dir with test dataset chunks')
    parser.add_argument('-e', '--epochs', default=3, help='epochs count')
    parser.add_argument('-b', '--batch_size', default=32, help='batch size')
    parser.add_argument('-c', '--cost_tflops', default=100, help='train cost in tflops')

    args = parser.parse_args()

    benchmark_train(
        train_dir=args.dataset_train,
        test_dir=args.dataset_test,
        model_path=args.model_path,
        batch_size=args.batch_size,
        epochs=args.epochs,
        cost_tflops=args.cost_tflops
    )
