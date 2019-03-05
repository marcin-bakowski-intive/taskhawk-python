import os
os.environ.setdefault("SETTINGS_MODULE", "examples.google_pubsub.user_settings")

from time import time
from .tasks import echo


def main():
    message = "Test echo: %s" % time()
    echo.dispatch(message)
    print(f"Published echo message: '{message}'")


if __name__ == "__main__":
    main()
