import itertools
from abc import ABC, abstractmethod

from werkzeug.datastructures import FileStorage


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


def chunk_reader(file, read_bytes: int):
    if not isinstance(file, FileStorage):
        file = open(file, "r")
    # Discard the header
    file.readline()
    while True:
        chunk = file.readlines(read_bytes)
        if not chunk:
            break
        yield chunk
    file.close()


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
