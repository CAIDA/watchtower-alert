import argparse
import json
import logging
import os
import pykafka

import watchtower.alert  # Alert, Error, Violation
import watchtower.alert.consumers


class Consumer:

    defaults = {
        "logging": "INFO",

        "critical_consumers": ["log"],
        "warning_consumers": ["log"],
        "normal_consumers": ["log"],
        "error_consumers": ["log"],

        "brokers": "localhost:9092",
        "topic_prefix": "watchtower",
        "alert_topic": "alerts",
        "error_topic": "errors",

        "consumers": {},
    }

    def __init__(self, config_file):
        self.config_file = os.path.expanduser(config_file)
        self.config = dict(self.defaults)
        self._load_config()

        self.consumer_classes = None
        self._init_plugins()

        self._init_consumers()

        # connect to kafka
        self.kc = pykafka.KafkaClient(hosts=self.config['brokers'])
        # set up our consumers
        self.alert_consumer =\
            self._topic(self.config['alert_topic'])\
                .get_simple_consumer(consumer_timeout_ms=5000)
        self.error_consumer =\
            self._topic(self.config['error_topic'])\
                .get_simple_consumer(consumer_timeout_ms=5000)

    def _init_plugins(self):
        self.consumer_classes = {
            "log": watchtower.alert.consumers.LogConsumer,
            # "email": watchtower.alert.consumers.EmailConsumer,
            # "database": watchtower.alert.consumers.DatabaseConsumer
        }

    def _load_config(self):
        with open(self.config_file) as fconfig:
            self.config.update(json.loads(fconfig.read()))
        self._configure_logging()
        logging.debug(self.config)

    def _configure_logging(self):
        logging.basicConfig(level=self.config.get('logging', 'info'),
                            format='%(asctime)s|CONSUMER|%(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def _topic(self, name):
        name = "%s-%s" % (self.config['topic_prefix'], name)
        return self.kc.topics[name.encode("ascii")]

    def _init_consumers(self):
        self.consumers = {}
        for level in watchtower.alert.Alert.LEVELS:
            cfg = self.config[level+'_consumers']
            self.consumers[level] = []
            for consumer in cfg:
                consumer_config = self.config['consumers'][consumer] if consumer in self.config['consumers'] else None
                self.consumers[level].append(self.consumer_classes[consumer](consumer_config))

    def _handle_alert(self, msg):
        for consumer in self.consumers[msg['level']]:
            consumer.handle_alert(msg)

    def _handle_error(self, msg):
        for consumer in self.consumers['error']:
            consumer.handle_error(msg)

    def run(self):
        # loop forever consuming alerts
        while True:
            for msg in self.alert_consumer:
                if msg is not None:
                    self._handle_alert(watchtower.alert.Alert.from_json(msg.value))
            for msg in self.error_consumer:
                if msg is not None:
                    self._handle_error(watchtower.alert.Error.from_json(msg.value))


def main():
    parser = argparse.ArgumentParser(description="""
    Consumes Watchtower alerts from Kafka and dispatches them to a chain
    of consumer plugins
    """)
    parser.add_argument('-c',  '--config-file',
                        nargs='?', required=True,
                        help='Config file')

    opts = vars(parser.parse_args())

    server = Consumer(**opts)
    server.run()
