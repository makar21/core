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
                func(*args)
            except urllib3.exceptions.ProtocolError as ex:
                logger.debug(ex)

                retry_count -= 1
                if retry_count == 0:
                    raise
                time.sleep(10)
            except bigchaindb_driver.exceptions.TransportError as ex:
                logger.debug(ex)

                if isinstance(ex, bigchaindb_driver.exceptions.BadRequest):
                    if 'already exists' in ex.info['message'] or 'DoubleSpend' in ex.info['message']:
                        return

                retry_count -= 1
                if retry_count == 0:
                    raise
                time.sleep(3)
            except Exception as ex:
                logger.debug(ex)

                retry_count -= 1
                if retry_count == 0:
                    raise
                time.sleep(3)

    return wrapper


class TatauTransactionsEndpoint(TransactionsEndpoint):
    @handle_bdb_exceptions
    def send_commit(self, transaction, headers=None):
        return super(TatauTransactionsEndpoint, self).send_commit(transaction, headers)

    @handle_bdb_exceptions
    def send_async(self, transaction, headers=None):
        return super(TatauTransactionsEndpoint, self).send_async(transaction, headers)


class TatauBigchainDB(BigchainDB):
    def __init__(self, *args, **kwargs):
        super(TatauBigchainDB, self).__init__(*args, **kwargs)
        self._transactions = TatauTransactionsEndpoint(self)
