from logging import getLogger

from tatau_core.nn import benchmark
from tatau_core.utils.logging import configure_logging

configure_logging(__name__)

logger = getLogger(__name__)


if __name__ == '__main__':
    logger.info('Start benchmark')
    benchmark.run()
