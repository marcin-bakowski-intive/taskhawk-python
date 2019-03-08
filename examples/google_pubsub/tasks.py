import taskhawk


@taskhawk.task
def echo(message):
    print("Echo '%s'" % message)


@taskhawk.task
def echo_with_extended_visibility_timeout(message, timeout, metadata=None):
    consumer_backend = metadata['consumer_backend']
    priority = metadata.pop("priority")
    consumer_backend.extend_visibility_timeout(priority, timeout, **metadata)
    print("Echo '%s'" % message)
