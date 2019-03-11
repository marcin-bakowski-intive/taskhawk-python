import os

os.environ.setdefault("SETTINGS_MODULE", "examples.google_pubsub.user_settings")

import taskhawk  # noqa
from taskhawk import Priority  # noqa

# collect echo task
from .tasks import echo  # noqa


def main():
    print("Starting Google PubSub subscriber")
    taskhawk.listen_for_messages(Priority.default)


if __name__ == "__main__":
    main()
