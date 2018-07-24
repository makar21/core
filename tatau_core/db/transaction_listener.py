import json
from logging import getLogger

import websocket

from tatau_core import settings
from .exceptions import StopWSClient

logger = getLogger()


class TransactionListener:
    def on_message(self, ws, message):
        data = json.loads(message)
        try:
            self.process_tx(data)
        except StopWSClient:
            ws.close()

    def on_error(self, ws, error):
        logger.error(error)

    def on_close(self, ws):
        logger.info('WS connection closed')

    def on_open(self, ws):
        logger.info('WS connection opened')

    def process_tx(self, data):
        raise NotImplemented

    def run_transaction_listener(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            settings.VALID_TRANSACTIONS_STREAM_URL,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.on_open = self.on_open
        ws.run_forever()
