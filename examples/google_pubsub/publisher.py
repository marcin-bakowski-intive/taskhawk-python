import os
os.environ.setdefault("SETTINGS_MODULE", "examples.google_pubsub.user_settings")

from time import time
from .tasks import echo, echo_with_extended_visibility_timeout


def main():
    message = f"Test echo: {time()}"
    echo.dispatch(message)
    print(f"Published echo message: '{message}'")

    visibility_timeout = 60
    message = f"{message} - with extended visibility_timeout={visibility_timeout}"
    echo_with_extended_visibility_timeout.dispatch(message, visibility_timeout)
    print(f"Published echo message: '{message}'")


if __name__ == "__main__":
    main()
