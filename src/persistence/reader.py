from abc import ABC, abstractmethod


class Reader(ABC):
    """
    Reader abstract class. All readers must implement all these methods so they are as similar as possible.
    Keep as much methods as we can here.
    """

    @abstractmethod
    def header_date(self, header):
        pass

    @abstractmethod
    def header_time(self, header):
        pass

    @abstractmethod
    def header_nodes(self, header):
        pass

    @abstractmethod
    def header_apps(self, header):
        pass

    @abstractmethod
    def header_parser(self, header):
        pass

    @abstractmethod
    def parse_file(self, file):
        pass
