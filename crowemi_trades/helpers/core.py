from datetime import datetime, timezone


def debug(func):
    def wrapper(*args, **kwargs):
        print(func.__name__)
        return func(*args, **kwargs)
