import sys

EXIT_FAILURE = -1
DEBUG = 1

def eprint(*args, **kwargs):
    ''' Error handler '''
    print(*args, file=sys.stderr, **kwargs)
    sys.exit(EXIT_FAILURE)


def dprint(*args, **kwargs):
    ''' Debug handler '''
    print(*args, file=sys.stderr, **kwargs)