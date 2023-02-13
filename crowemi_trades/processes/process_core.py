from abc import ABC, abstractclassmethod


class ProcessCore(ABC):
    """Base class for processes."""

    @staticmethod
    @abstractclassmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError("Base class has no run procedure defined.")
