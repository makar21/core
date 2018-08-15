class StopWSClient(Exception):
    pass


class Asset:
    class WrongType(Exception):
        pass

    class NotFound(Exception):
        pass


class NodeNotConfigured(Exception):
    pass
