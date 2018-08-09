import importlib


def load_class(class_path: str):
    """
    Load Class from module
    :param class_path: class name including module path for example "tatau_core.nn.torch.serializer.WeightsSerializer"
    :return: class
    """
    class_items = class_path.split(".")
    class_name = class_items[-1]
    module_name = ".".join(class_items[:-1])
    module = importlib.import_module(module_name)
    return getattr(module, class_name)
