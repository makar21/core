from tatau.node.worker import Worker
import settings
from raven import Client

client = Client(settings.RAVEN_DSN)

if __name__ == '__main__':
    try:
        w = Worker(fs_keys_name='worker')
        w.run_transaction_listener()
    except:
        client.captureException()
