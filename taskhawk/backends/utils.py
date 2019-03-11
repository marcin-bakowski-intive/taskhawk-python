import importlib

from taskhawk import Priority
from taskhawk.conf import settings


def get_queue_name(priority: Priority) -> str:
    name = f'TASKHAWK-{settings.TASKHAWK_QUEUE.upper()}'
    if priority is Priority.high:
        name += '-HIGH-PRIORITY'
    elif priority is Priority.low:
        name += '-LOW-PRIORITY'
    elif priority is Priority.bulk:
        name += '-BULK'
    return name


def import_class(dotted_path):
    if not dotted_path:
        raise ImportError(f"{dotted_path} is not defined")
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as err:
        raise ImportError(f"{dotted_path} doesn't look like a module path") from err

    module = importlib.import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError as err:
        raise ImportError(f"Module '{module_path}' does not define a '{class_name}' attribute/class") from err
