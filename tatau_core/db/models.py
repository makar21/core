from tatau_core import settings
from tatau_core.db import exceptions
from tatau_core.db.asset import Asset
from tatau_core.db.fields import Field


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
    def __init__(self, db, encryption, asset=None, _decrypt_values=False,
                 created_at=None, modified_at=None, public_key=None, **kwargs):
        # param "_decrypt_values" was added for using in methods get, history, because when data loads from db,
        # then data should be decrypted, but when new instance is creating, then data which passed to constructor
        # is not encrypted
        self.db = db
        self.encryption = encryption
        self.asset = asset
        self._public_key = public_key
        self._created_at = created_at
        self._modified_at = modified_at

        for name, attr in self._attrs.items():
            if isinstance(attr, Field):
                value = kwargs[name] if name in kwargs else attr.initial
                attr.set_value(self, value, name=name, encryption=self.encryption, decrypt=_decrypt_values)

    def __str__(self):
        return '<{}: {}>'.format(self.get_asset_name(), self.asset_id)

    @property
    def asset_id(self):
        if self.asset:
            return self.asset.asset_id
        return None

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
        return self.asset.address

    def get_encryption_key(self):
        return self._public_key

    def set_encryption_key(self, public_key):
        self._public_key = public_key

    @classmethod
    def get_asset_name(cls):
        return cls._asset_name + settings.RING_NAME

    def _prepare_value(self, name, attr):
        value = getattr(self, name)
        return attr.prepare_value(value, public_key=self.get_encryption_key())

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
    def get(cls, asset_id, db, encryption):
        asset = Asset.get(asset_id, db)
        if asset.data['asset_name'] != cls.get_asset_name():
            raise exceptions.Asset.WrongType()

        kwargs = dict(asset=asset)
        kwargs.update(asset.data)
        if asset.metadata is not None:
            kwargs.update(asset.metadata)
        kwargs['created_at'] = asset.created_at
        kwargs['modified_at'] = asset.modified_at
        return cls(db=db, encryption=encryption, _decrypt_values=True, **kwargs)

    @classmethod
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        obj.save(recipients=kwargs.get('recipients'))
        return obj

    def save(self, recipients=None):
        if self.asset is not None:
            self.asset.save(
                metadata=self.get_metadata(),
                recipients=recipients,
            )
        else:
            self.asset, created = Asset.create(
                data=self.get_data(),
                metadata=self.get_metadata(),
                recipients=recipients,
                db=self.db
            )

    @classmethod
    def enumerate(cls, db, encryption, additional_match=None, created_by_user=True, limit=None, skip=None):
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
    def list(cls, db, encryption, additional_match=None, created_by_user=True, limit=None, skip=None):
        return list(cls.enumerate(db, encryption, additional_match, created_by_user, limit, skip))

    @classmethod
    def exists(cls, db, additional_match=None, created_by_user=True):
        return cls.count(db, additional_match, created_by_user) > 0

    @classmethod
    def count(cls, db, additional_match=None, created_by_user=True):
        db.connect_to_mongodb()
        match = {
            'assets.data.asset_name': cls.get_asset_name(),
        }

        if additional_match is not None:
            match.update(additional_match)

        return db.retrieve_asset_count(match=match, created_by_user=created_by_user)

    @classmethod
    def get_history(cls, asset_id, db, encryption):
        data = None
        created_at = None
        for transaction in db.get_transactions(asset_id=asset_id):
            if transaction['operation'] == 'CREATE':
                data = transaction['asset']['data']
                created_at = transaction['generation_time']
                if data['asset_name'] != cls.get_asset_name():
                    raise exceptions.Asset.WrongType()

            metadata = transaction['metadata']
            kwargs = data
            kwargs.update(metadata)
            kwargs['created_at'] = created_at
            kwargs['modified_at'] = transaction['generation_time']
            # db = None to be sure save() for returned object will be failed
            yield cls(db=None, encryption=encryption, _decrypt_values=True, **kwargs)

    @classmethod
    def get_with_initial_data(cls, asset_id, db, encryption):
        asset = Asset.get(asset_id=asset_id, db=db)
        if asset.data['asset_name'] != cls.get_asset_name():
            raise exceptions.Asset.WrongType()

        kwargs = dict(asset=asset)
        kwargs.update(asset.data)
        if asset.initial_metadata is not None:
            kwargs.update(asset.initial_metadata)

        kwargs['created_at'] = asset.created_at
        kwargs['modified_at'] = asset.modified_at
        return cls(db=db, encryption=encryption, _decrypt_values=True, **kwargs)
