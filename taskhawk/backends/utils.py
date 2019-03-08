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
