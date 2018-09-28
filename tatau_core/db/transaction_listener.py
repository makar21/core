import json
from logging import getLogger

import websocket

from tatau_core import settings
from .exceptions import StopWSClient

logger = getLogger('tatau_core')


# noinspection PyMethodMayBeStatic
class TransactionListener:
    def _on_message(self, ws, message):
        data = json.loads(message)
        try:
            self._process_tx(data)
        except StopWSClient:
            ws.close()

    def _on_error(self, ws, error):
        logger.error(error)

    def _on_close(self, ws):
        logger.info('WS connection closed')

    def _on_open(self, ws):
        logger.info('WS connection opened')

    def _process_tx(self, data):
        raise NotImplemented

    def run_transaction_listener(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            settings.VALID_TRANSACTIONS_STREAM_URL,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        ws.on_open = self._on_open
        ws.run_forever()
