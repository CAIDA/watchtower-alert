import argparse
import json
import logging
import os
import confluent_kafka
import time

from .alert import Alert
from .consumers import *


class Consumer:

    defaults = {
        "logging": "INFO",

        "alert_consumers": ["log"],
        "timer_consumers": ["log"],
        "consumer_cfgs": {},

        "brokers": "localhost:9092",
        "topic": "watchtower",

        "timer_interval": 60,

        "consumers": {}
    }

    def __init__(self, config_file):
        self.config_file = os.path.expanduser(config_file)
        self.config = dict(self.defaults)
        self._load_config()
        self.topic = self.config['topic']

        self.next_timer = None

        self.consumer_instances = None
        self._init_plugins()

        self.consumers = None
        self._init_consumers()

        # connect to kafka
        kafka_conf = {
            'bootstrap.servers': self.config['brokers'],
            'group.id': self.config['consumer_group'],
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'heartbeat.interval.ms': 60000,
            'api.version.request': True,
        }
        self.kc = confluent_kafka.Consumer(**kafka_conf)
        logging.info("Subscribing to alerts from '%s'" % self.topic)
        self.kc.subscribe([self.topic])

    def _init_plugins(self):
        consumers = {
            "log": LogConsumer,
            "database": DatabaseConsumer,
            # "traceroute": watchtower.alert.consumers.TracerouteConsumer,
            "timeseries": TimeseriesConsumer,
            "slack": SlackConsumer,
        }
        self.consumer_instances = {}
        for consumer, clz in list(consumers.items()):
            cfg = self.config['consumers'].get(consumer, None)
            self.consumer_instances[consumer] = clz(cfg)

    def _load_config(self):
        with open(self.config_file) as fconfig:
            self.config.update(json.loads(fconfig.read()))
        self._configure_logging()
        # logging.debug(self.config)

    def _configure_logging(self):
        logging.basicConfig(level=self.config.get('logging', 'info'),
                            format='%(asctime)s|WATCHTOWER|%(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def _init_consumers(self):
        self.consumers = {}
        for alert_type in ['alert', 'timer']:
            cfg = self.config[alert_type + '_consumers']
            self.consumers[alert_type] = []
            for cons_name in cfg:
                cons_inst = self.consumer_instances[cons_name]
                cons_inst.start()
                self.consumers[alert_type].append(cons_inst)

    def _handle_alert(self, msg):
        try:
            alert = Alert.from_json(msg.value())
        except (TypeError, ValueError) as e:
            logging.error("Could not extract Alert from json: %s" % msg.value())
            logging.exception(e)
            return
        for consumer in self.consumers['alert']:
            consumer.handle_alert(alert)

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

            # ALERTS
            msg = self.kc.poll(10)
            eof_since_data = 0
            while msg is not None:
                if not msg.error():
                    self._handle_alert(msg)
                    eof_since_data = 0
                elif msg.error().code() == \
                        confluent_kafka.KafkaError._PARTITION_EOF:
                    # no new messages, wait a bit and then drop out and check timers
                    eof_since_data += 1
                    if eof_since_data >= 6:
                        break
                else:
                    logging.error("Unhandled Kafka error: %s" % msg.error())
                msg = self.kc.poll(10)


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
