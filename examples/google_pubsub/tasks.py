import taskhawk


@taskhawk.task
def echo(message):
    print("Echo '%s'" % message)
