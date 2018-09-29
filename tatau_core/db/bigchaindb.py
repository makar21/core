import time
from logging import getLogger

import bigchaindb_driver.exceptions
import urllib3
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.driver import TransactionsEndpoint

logger = getLogger('tatau_core')


def handle_bdb_exceptions(func):
    def wrapper(*args):
        retry_count = 10
        while retry_count > 0:
            try:
                return func(*args)
            except urllib3.exceptions.ProtocolError as ex:
                logger.warning("TX {} error: {}".format(args[1]['id'], ex))

                retry_count -= 1
                if retry_count == 0:
                    raise
                time.sleep(1)
            except bigchaindb_driver.exceptions.TransportError as ex:
                logger.warning("TX {} error: {}".format(args[1]['id'], ex))

                if isinstance(ex, bigchaindb_driver.exceptions.BadRequest):
                    if 'DuplicateTransaction' in ex.info['message'] or 'DoubleSpend' in ex.info['message']:
                        return args[1]

                retry_count -= 1
                if retry_count == 0:
                    raise
                time.sleep(1)
            except Exception as ex:
                logger.warning("TX {} error: {}".format(args[1]['id'], ex))

                retry_count -= 1
                if retry_count == 0:
                    raise
                time.sleep(1)

    return wrapper


class TatauTransactionsEndpoint(TransactionsEndpoint):
    @handle_bdb_exceptions
    def send_commit(self, transaction, headers=None):
        logger.debug("Send commit TX: {}".format(transaction['id']))
        return super(TatauTransactionsEndpoint, self).send_commit(transaction, headers)

    @handle_bdb_exceptions
    def send_async(self, transaction, headers=None):
        logger.debug("Send async TX: {}".format(transaction['id']))
        return super(TatauTransactionsEndpoint, self).send_async(transaction, headers)


class TatauBigchainDB(BigchainDB):
    def __init__(self, *args, **kwargs):
        super(TatauBigchainDB, self).__init__(*args, **kwargs)
        self._transactions = TatauTransactionsEndpoint(self)
