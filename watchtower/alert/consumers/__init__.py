import abc


class AbstractConsumer:
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.config = config

    @abc.abstractmethod
    def handle_alert(self, alert):
        pass

    @abc.abstractmethod
    def handle_error(self, error):
        pass


# When adding a consumer here, also add to _init_plugins method in
# watchtower.alert.consumer.py
# TODO: make adding consumer more dynamic
from watchtower.alert.consumers.log import LogConsumer
# from watchtower.alert.consumers.email import EmailConsumer
# from watchtower.alert.consumers.database import DatabaseConsumer
from watchtower.alert.consumers.traceroute import TracerouteConsumer
