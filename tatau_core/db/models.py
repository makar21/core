import json

from tatau_core.db.fields import Field, JsonField
from tatau_core.db import exceptions, NodeInfo


class ModelBase(type):
    """Metaclass for all models."""
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__

        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return super_new(cls, name, bases, attrs)

        # Create the class.
        new_class = super_new(cls, name, bases, attrs)
        new_class._asset_name = name
        new_class._attrs = attrs
        return new_class


class Model(metaclass=ModelBase):
    def __init__(self, db=None, encryption=None, **kwargs):
        self.db = db or NodeInfo.get_db()
        self.encryption = encryption or NodeInfo.get_encryption()
        self.asset_id = kwargs.get('asset_id', None)
        self._address = kwargs.get('_address', None)
        self._public_key = None

        for name, attr in self._attrs.items():
            if isinstance(attr, Field):
                attr._name = name
                value = kwargs[name] if name in kwargs else attr.initial
                if attr.encrypted and kwargs.get('_decrypt_values', False):
                    value = self.encryption.decrypt_text(value)
                if isinstance(attr, JsonField) and isinstance(value, str):
                    value = json.loads(value)
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
    def address(self):
        return self._address

    def get_encryption_key(self):
        return self._public_key

    def set_encryption_key(self, public_key):
        self._public_key = public_key

    @classmethod
    def get_asset_name(cls):
        return cls._asset_name

    def _prepare_value(self, name, attr):
        value = getattr(self, name)

        if value is None and not attr.null and attr.required:
            raise ValueError('{} is required'.format(name))

        if attr.encrypted:
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
        db = db or NodeInfo.get_db()
        asset = db.retrieve_asset(asset_id)
        address = asset.tx['outputs'][0]['public_keys'][0]

        if asset.data['asset_name'] != cls.get_asset_name():
            raise exceptions.Asset.WrongType()

        kwars = dict(asset_id=asset_id)
        kwars.update(asset.data)
        if asset.metadata is not None:
            kwars.update(asset.metadata)
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
    def list(cls, db=None, encryption=None, additional_match=None, created_by_user=True):
        db = db or NodeInfo.get_db()
        db.connect_to_mongodb()
        match = {
            'assets.data.asset_name': cls.get_asset_name(),
        }

        if additional_match is not None:
            match.update(additional_match)
        return (cls.get(x, db, encryption) for x in db.retrieve_asset_ids(match=match, created_by_user=created_by_user))

    @classmethod
    def exists(cls, db=None, encryption=None, additional_match=None, created_by_user=True):
        for v in cls.list(db, encryption, additional_match, created_by_user):
            return True
        return False

    @classmethod
    def count(cls, db=None, additional_match=None, created_by_user=True):
        db = db or NodeInfo.get_db()
        db.connect_to_mongodb()
        match = {
            'assets.data.asset_name': cls.get_asset_name(),
        }

        if additional_match is not None:
            match.update(additional_match)
        return len(list(db.retrieve_asset_ids(match=match, created_by_user=created_by_user)))



