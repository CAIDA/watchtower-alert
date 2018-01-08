import argparse
import json
import logging
import os
import confluent_kafka
import time

import watchtower.alert  # Alert, Error, Violation
import watchtower.alert.consumers


class Consumer:

    defaults = {
        "logging": "INFO",

        "critical_consumers": ["log"],
        "warning_consumers": ["log"],
        "normal_consumers": ["log"],
        "error_consumers": ["log"],
        "timer_consumers": ["log"],

        "brokers": "localhost:9092",
        "topic_prefix": "watchtower",
        "alert_topic": "alerts",
        "error_topic": "errors",

        "timer_interval": 60,

        "consumers": {},
    }

    def __init__(self, config_file):
        self.config_file = os.path.expanduser(config_file)
        self.config = dict(self.defaults)
        self._load_config()

        self.next_timer = None

        self.consumer_instances = None
        self._init_plugins()

        self.consumers = None
        self._init_consumers()

        # connect to kafka
        kafka_conf = {
            'bootstrap.servers': self.config['brokers'],
            'group.id': self.config['consumer_group'],
            'default.topic.config': {'auto.offset.reset': 'latest'},
            'heartbeat.interval.ms': 60000,
            'api.version.request': True,
        }
        self.kc = confluent_kafka.Consumer(**kafka_conf)
        self.kc.subscribe([
            self.config['alert_topic'],
            self.config['error_topic']
        ])
        self.msg_handlers = {
            self.config['alert_topic']: self._handle_alert,
            self.config['error_topic']: self._handle_error,
        }

    def _init_plugins(self):
        consumers = {
            "log": watchtower.alert.consumers.LogConsumer,
            # "email": watchtower.alert.consumers.EmailConsumer,
            "database": watchtower.alert.consumers.DatabaseConsumer,
            # "traceroute": watchtower.alert.consumers.TracerouteConsumer,
            "timeseries": watchtower.alert.consumers.TimeseriesConsumer,
        }
        self.consumer_instances = {}
        for consumer, clz in consumers.iteritems():
            cfg = self.config['consumers'][consumer] \
                if consumer in self.config['consumers'] else None
            self.consumer_instances[consumer] = clz(cfg)

    def _load_config(self):
        with open(self.config_file) as fconfig:
            self.config.update(json.loads(fconfig.read()))
        self._configure_logging()
        logging.debug(self.config)

    def _configure_logging(self):
        logging.basicConfig(level=self.config.get('logging', 'info'),
                            format='%(asctime)s|WATCHTOWER|%(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def _topic(self, name):
        name = "%s-%s" % (self.config['topic_prefix'], name)
        return self.kc.topics[name.encode("ascii")]

    def _init_consumers(self):
        self.consumers = {}
        for level in watchtower.alert.Alert.LEVELS + ['timer']:
            cfg = self.config[level+'_consumers']
            self.consumers[level] = []
            for consumer in cfg:
                self.consumers[level].append(self.consumer_instances[consumer])

    def _handle_alert(self, msg):
        try:
            alert = watchtower.alert.Alert.from_json(msg.value)
        except ValueError:
            logging.error("Could not extract Alert from json: %s" % msg.value)
            return
        for consumer in self.consumers[alert.level]:
            consumer.handle_alert(alert)

    def _handle_error(self, msg):
        try:
            error = watchtower.alert.Error.from_json(msg.value)
        except ValueError:
            logging.error("Could not extract Error from json: %s" % msg.value)
            return
        for consumer in self.consumers['error']:
            consumer.handle_error(error)

    def _handle_timer(self, now):
        for consumer in self.consumers['timer']:
            consumer.handle_timer(now)

    def run(self):
        # loop forever consuming alerts
        while True:
            # TIMERS
            now = time.time()
            if not self.next_timer or now >= self.next_timer:
                if self.next_timer:
                    self._handle_timer(now)
                    self.next_timer += self.config['timer_interval']
                else:
                    interval = self.config['timer_interval']
                    self.next_timer = (int(now/interval) * interval) + interval

            # ALERTS and ERRORS
            msg = self.kc.poll(1000)
            eof_since_data = 0
            while msg is not None:
                if not msg.error():
                    self.msg_handlers[msg.topic](msg)
                    eof_since_data = 0
                elif msg.error().code() == \
                        confluent_kafka.KafkaError._PARTITION_EOF:
                    # no new messages, wait a bit and then drop out and check timers
                    eof_since_data += 1
                    if eof_since_data >= 10:
                        break
                else:
                    logging.error("Unhandled Kafka error: %s" % msg.error())
                msg = self.kc.poll(1000)


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
