def singleton(class_):
    class ClassW(class_):
        _instance = None

        def __new__(cls, *args, **kwargs):
            if ClassW._instance is None:
                ClassW._instance = super(ClassW, cls).__new__(cls, *args, **kwargs)
                ClassW._instance._sealed = False
            return ClassW._instance

        def __init__(self, *args, **kwargs):
            if self._sealed:
                return
            super(ClassW, self).__init__(*args, **kwargs)
            self._sealed = True

    ClassW.__name__ = class_.__name__
    return ClassW
