import os
os.environ.setdefault("SETTINGS_MODULE", "examples.google_pubsub.user_settings")

import taskhawk
from taskhawk import Priority

# collect echo task
from .tasks import echo


def main():
    print("Starting Google PubSub subscriber")
    taskhawk.listen_for_messages(Priority.default)


if __name__ == "__main__":
    main()
