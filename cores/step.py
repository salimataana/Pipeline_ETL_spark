from abc import abstractmethod


class Step:

    @abstractmethod
    def execute(self, data):
        pass