from web3 import Web3

def tatau_web3():
    from tatau_core import settings
    # provider = Web3.WebsocketProvider("ws://{}:{}".format(settings.PARITY_HOST, settings.PARITY_WEBSOCKET_PORT))
    provider = Web3.HTTPProvider(
        endpoint_uri="http://{}:{}".format(settings.PARITY_HOST, settings.PARITY_JSONRPC_PORT),
        request_kwargs=dict(timeout=60)
    )
    return Web3(provider)


web3 = tatau_web3()
