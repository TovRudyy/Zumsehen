import itertools
from abc import ABC, abstractmethod


def isplit(iterable, part_size, group=list):
    """ Yields groups of length `part_size` of items found in iterator.
    group is a constructor which transforms an iterator into a object
    with `part_size` or less elements (example: list, tuple or set)
    """
    iterator = iter(iterable)
    while True:
        tmp = group(itertools.islice(iterator, 0, part_size))
        if not tmp:
            return
        yield tmp


def chunk_reader(filename: str, read_bytes: int):
    with open(filename, "r") as f:
        # Discard the header
        f.readline()
        while True:
            chunk = f.readlines(read_bytes)
            if not chunk:
                break
            yield chunk


class FormatConverter(ABC):
    @abstractmethod
    def parse_records(self, chunk):
        pass

    @abstractmethod
    def parse_as_dataframe(self, file: str):
        pass

    @abstractmethod
    def seq_parser(self, chunk):
        pass
