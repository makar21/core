import json

from tatau_core import settings
from tatau_core.db import exceptions, NodeDBInfo
from tatau_core.db.fields import Field, JsonField, EncryptedJsonField


class ModelBase(type):
    """Metaclass for all models."""
    def __new__(mcs, name, bases, attrs):
        super_new = super().__new__

        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return super_new(mcs, name, bases, attrs)

        # Create the class.
        new_class = super_new(mcs, name, bases, attrs)
        new_class._asset_name = name
        new_class._attrs = attrs
        return new_class


class Model(metaclass=ModelBase):
    def __init__(self, db=None, encryption=None, asset_id=None, _address=None, _decrypt_values=False,
                 created_at=None, modified_at=None, **kwargs):
        # param "_decrypt_values" was added for using in methods get, history, because when data loads from db,
        # then data should be decrypted, but when new instance is creating, then data which passed to constructor
        # is not encrypted
        self.db = db or NodeDBInfo.get_db()
        self.encryption = encryption or NodeDBInfo.get_encryption()
        self.asset_id = asset_id
        self._address = _address
        self._public_key = None
        self._created_at = created_at
        self._modified_at = modified_at

        for name, attr in self._attrs.items():
            if isinstance(attr, Field):
                attr._name = name
                value = kwargs[name] if name in kwargs else attr.initial
                if attr.encrypted and _decrypt_values:
                    value = self.encryption.decrypt_text(value)
                if isinstance(attr, JsonField) and isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        if attr.encrypted:
                            value = value
                        else:
                            raise
                attr.__set__(self, value)

    def __str__(self):
        return '<{}: {}>'.format(self.get_asset_name(), self.asset_id)

    @classmethod
    def get_fields(cls):
        fields = []
        for name, attr in cls._attrs.items():
            if isinstance(attr, Field):
                fields.append({
                    'name': name,
                    'class': attr
                })
        return fields

    @property
    def created_at(self):
        return self._created_at

    @property
    def modified_at(self):
        return self._modified_at

    @property
    def address(self):
        return self._address

    def get_encryption_key(self):
        return self._public_key

    def set_encryption_key(self, public_key):
        self._public_key = public_key

    @classmethod
    def get_asset_name(cls):
        return cls._asset_name + settings.RING_NAME

    def _prepare_value(self, name, attr):
        value = getattr(self, name)

        if value is None and not attr.null and attr.required:
            raise ValueError('{} is required'.format(name))

        if attr.encrypted:
            if isinstance(attr, EncryptedJsonField):
                value = json.dumps(value)
            return self.encryption.encrypt_text(value, self.get_encryption_key())

        if isinstance(attr, JsonField):
            return json.dumps(value)

        return value

    def get_data(self):
        data = dict(asset_name=self.get_asset_name())
        for name, attr in self._attrs.items():
            if isinstance(attr, Field) and attr.immutable:
                data[name] = self._prepare_value(name, attr)
        return data

    def get_metadata(self):
        metadata = dict()
        for name, attr in self._attrs.items():
            if isinstance(attr, Field) and not attr.immutable:
                metadata[name] = self._prepare_value(name, attr)
        return metadata or None

    @classmethod
    def get(cls, asset_id, db=None, encryption=None):
        db = db or NodeDBInfo.get_db()
        encryption = encryption or NodeDBInfo.get_encryption()

        asset = db.retrieve_asset(asset_id)
        address = asset.last_tx['outputs'][0]['public_keys'][0]

        if asset.data['asset_name'] != cls.get_asset_name():
            raise exceptions.Asset.WrongType()

        kwars = dict(asset_id=asset_id)
        kwars.update(asset.data)
        if asset.metadata is not None:
            kwars.update(asset.metadata)
        kwars['created_at'] = asset.created_at
        kwars['modified_at'] = asset.modified_at
        return cls(db=db, encryption=encryption, _decrypt_values=True, _address=address, **kwars)

    @classmethod
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        obj.save(recipients=kwargs.get('recipients'))
        return obj

    def save(self, recipients=None):
        if self.asset_id is not None:
            self.db.update_asset(
                asset_id=self.asset_id,
                metadata=self.get_metadata(),
                recipients=recipients,
            )
        else:
            self.asset_id, created = self.db.create_asset(
                data=self.get_data(),
                metadata=self.get_metadata(),
                recipients=recipients
            )

    @classmethod
    def enumerate(cls, db=None, encryption=None, additional_match=None, created_by_user=True, limit=None, skip=None):
        db = db or NodeDBInfo.get_db()
        encryption = encryption or NodeDBInfo.get_encryption()

        db.connect_to_mongodb()
        match = {
            'assets.data.asset_name': cls.get_asset_name(),
        }

        if additional_match is not None:
            match.update(additional_match)
        return (
            cls.get(x, db, encryption)
            for x in db.retrieve_asset_ids(match=match, created_by_user=created_by_user, limit=limit, skip=skip)
        )

    @classmethod
    def list(cls, db=None, encryption=None, additional_match=None, created_by_user=True, limit=None, skip=None):
        return list(cls.enumerate(db, encryption, additional_match, created_by_user, limit, skip))

    @classmethod
    def exists(cls, db=None, additional_match=None, created_by_user=True):
        return cls.count(db, additional_match, created_by_user) > 0

    @classmethod
    def count(cls, db=None, additional_match=None, created_by_user=True):
        db = db or NodeDBInfo.get_db()

        db.connect_to_mongodb()
        match = {
            'assets.data.asset_name': cls.get_asset_name(),
        }

        if additional_match is not None:
            match.update(additional_match)

        return db.retrieve_asset_count(match=match, created_by_user=created_by_user)

    @classmethod
    def get_history(cls, asset_id, db=None, encryption=None):
        db = db or NodeDBInfo.get_db()
        encryption = encryption or NodeDBInfo.get_encryption()

        data = None
        created_at = None
        for transaction in db.retrieve_asset_transactions(asset_id):
            if transaction['operation'] == 'CREATE':
                data = transaction['asset']['data']
                created_at = transaction['generation_time']
                if data['asset_name'] != cls.get_asset_name():
                    raise exceptions.Asset.WrongType()

            metadata = transaction['metadata']
            address = transaction['outputs'][0]['public_keys'][0]

            kwars = data
            kwars.update(metadata)
            kwars['created_at'] = created_at
            kwars['modified_at'] = transaction['generation_time']
            yield cls(db=db, encryption=encryption, asset_id=asset_id, _decrypt_values=True, _address=address, **kwars)


