from tatau_core.tatau.node.worker import Worker
from tatau_core import settings
from raven import Client

client = Client(settings.RAVEN_DSN)

if __name__ == '__main__':
    try:
        w = Worker(rsa_pk_fs_name='worker')
        w.run_transaction_listener()
    except:
        client.captureException()
