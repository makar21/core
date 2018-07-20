

class Field:
    encrypted = False

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


class CharField(Field):
    def __set__(self, obj, val):
        if val is not None and not isinstance(val, str):
            raise ValueError('{} must be a str instance.'.format(self._name))

        super(CharField, self).__set__(obj, val if val is None else val)


class EncryptedCharField(CharField):
    encrypted = True


class IntegerField(Field):
    def __set__(self, obj, val):
        if val is not None and not isinstance(val, int):
            raise ValueError('{} must be a integer instance.'.format(self._name))

        super(IntegerField, self).__set__(obj, val if val is None else val)


class JsonField(Field):
    pass
