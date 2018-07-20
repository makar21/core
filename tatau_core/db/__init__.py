from .db import DB
from .transaction_listener import TransactionListener
from .exceptions import NodeNotConfigured


class NodeInfo:
    _db = None
    _encryption = None

    @classmethod
    def configure(cls, db_instance, encryption_instance):
        cls._db = db_instance
        cls._encryption = encryption_instance

    @classmethod
    def get_db(cls):
        if cls._db is None:
            raise NodeNotConfigured
        else:
            return cls._db

    @classmethod
    def get_encryption(cls):
        if cls._encryption is None:
            raise NodeNotConfigured
        else:
            return cls._encryption
