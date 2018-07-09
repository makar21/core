import logging

from raven import Client
from raven.handlers.logging import SentryHandler

from tatau_core import settings


def configure_logging(name):

    client = Client(settings.RAVEN_DSN)

    logging.basicConfig(
        format='%(levelname)-8s [%(asctime)s] %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler('{}.log'.format(name)),
            logging.StreamHandler(),
            SentryHandler(client, level=logging.ERROR)
        ]
    )
