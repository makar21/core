import json


class Field:
    def __init__(self, initial=None, immutable=False, required=True, null=False):
        self._name = None

        self.initial = initial
        self.immutable = immutable
        self.required = required
        self.null = null

    def __get__(self, obj, obj_type):
        return getattr(obj, '_' + self._name)

    def __set__(self, obj, val):
        setattr(obj, '_' + self._name, val)

    def set_value(self, obj, val, name, *args, **kwargs):
        self._name = name
        return self.__set__(obj, val)

    def prepare_value(self, value, *args, **kwargs):
        if value is None and not self.null and self.required:
            raise ValueError('{} is required'.format(self._name))
        return value


class CharField(Field):
    def __set__(self, obj, val):
        if val is not None and not isinstance(val, str):
            raise ValueError('{} must be a str instance.'.format(self._name))

        super(CharField, self).__set__(obj, val if val is None else val)


class EncryptedCharField(CharField):
    def __init__(self, initial=None, immutable=False, required=True, null=False):
        super(EncryptedCharField, self).__init__(initial, immutable, required, null)
        self._encryption = None
        self._encrypt = True

    def __set__(self, instance, value):
        super(EncryptedCharField, self).__set__(instance, value)
        self._encrypt = True

    def set_value(self, obj, val, name, *args, **kwargs):
        self._name = name

        self._encryption = kwargs.get('encryption')
        _decrypt = kwargs.get('decrypt')

        assert self._encryption and _decrypt is not None

        encrypt = True

        if _decrypt:
            value, _decrypted = self._encryption.decrypt_text(val)
            if not _decrypted:
                encrypt = False
        else:
            value = val

        # set will reset _encrypt value to True, not decrypted value may be updated by operator "="
        self.__set__(obj, value)
        self._encrypt = encrypt

    def prepare_value(self, value, *args, **kwargs):
        value = super(EncryptedCharField, self).prepare_value(value, *args, **kwargs)
        if self._encrypt:
            value = self._encryption.encrypt_text(
                text=value,
                public_key=kwargs.get('public_key')
            )
        return value


class IntegerField(Field):
    def __set__(self, obj, val):
        if val is not None and not isinstance(val, int):
            raise ValueError('{} must be an integer instance.'.format(self._name))

        super(IntegerField, self).__set__(obj, val if val is None else val)


class JsonField(Field):
    def __set__(self, obj, val):
        if val is not None and not isinstance(val, dict) and not isinstance(val, list):
            raise ValueError('{} must be a dict or list instance.'.format(self._name))

        super(JsonField, self).__set__(obj, val if val is None else val)


class EncryptedJsonField(JsonField):
    def __init__(self, initial=None, immutable=False, required=True, null=False):
        super(EncryptedJsonField, self).__init__(initial, immutable, required, null)
        self._encryption = None
        self._encrypt = True

    def __set__(self, obj, val):
        if val is not None and not isinstance(val, dict) and not isinstance(val, list) and not isinstance(val, str):
            raise ValueError('{} must be a dict or str or list instance.'.format(self._name))
        super(JsonField, self).__set__(obj, val if val is None else val)
        self._encrypt = True

    def set_value(self, obj, val, name, *args, **kwargs):
        self._name = name

        self._encryption = kwargs.get('encryption')
        _decrypt = kwargs.get('decrypt')

        assert self._encryption and _decrypt is not None

        encrypt = True
        if _decrypt:
            value, _decrypted = self._encryption.decrypt_text(val)
            if not _decrypted:
                encrypt = False
            else:
                value = json.loads(value)
        else:
            value = val

        # set will reset _encrypt value to True, not decrypted value may be updated by operator "="
        self.__set__(obj, value)
        self._encrypt = encrypt

    def prepare_value(self, value, *args, **kwargs):
        value = super(EncryptedJsonField, self).prepare_value(value, *args, **kwargs)
        if value is not None and self._encrypt:
            value = self._encryption.encrypt_text(
                text=json.dumps(value),
                public_key=kwargs.get('public_key')
            )
        return value


# when transaction is CREATE this field will affect the creation of a new asset
class TimestampField(IntegerField):
    def __init__(self, *args, **kwargs):
        super(TimestampField, self).__init__(initial=None, immutable=False, required=False, null=True)


class FloatField(Field):
    def __set__(self, obj, val):
        if val is not None and not isinstance(val, float) and not isinstance(val, int):
            raise ValueError('{} must be an integer instance.'.format(self._name))

        super(FloatField, self).__set__(obj, val if val is None else float(val))
