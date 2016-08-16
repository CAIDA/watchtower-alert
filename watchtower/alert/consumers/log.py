import logging

from watchtower.alert.consumers import AbstractConsumer


class LogConsumer(AbstractConsumer):

    def handle_alert(self, alert):
        logging.debug("This is an alert")

    def handle_error(self, error):
        logging.debug("This is an error")

