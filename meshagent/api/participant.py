import logging


logger = logging.getLogger("agent")


class Participant:
    def __init__(self, *, id: str, attributes: dict):
        self._id = id
        self._attributes = attributes.copy()

    @property
    def id(self):
        return self._id

    def get_attribute(self, name: str):
        return self._attributes.get(name, None)
