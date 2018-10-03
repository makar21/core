import logging
import sys

from raven import Client
from raven.handlers.logging import SentryHandler

from tatau_core import settings


def configure_logging(name='tatau_core'):

    client = Client(settings.RAVEN_DSN)

    logging.basicConfig(
        format='%(asctime)s P%(process)d %(levelname)s |%(name)s| %(message)s',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(sys.stdout),
            SentryHandler(client, level=logging.ERROR)
        ],
    )

    logging.getLogger(name).setLevel(settings.TATAU_CORE_LOG_LVL)

