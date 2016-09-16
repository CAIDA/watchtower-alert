import pykafka


class Producer:

    def __init__(self,
                 brokers='localhost:9092',
                 topic_prefix='watchtower',
                 alert_topic='alerts',
                 error_topic='errors'):
        self.topic_prefix = topic_prefix

        # connect to kafka
        self.kc = pykafka.KafkaClient(hosts=brokers)
        # create topic handles
        self.alert_t = self._connect_topic(alert_topic)
        self.error_t = self._connect_topic(error_topic)

    def _connect_topic(self, topic_name):
        full_name = "%s-%s" % (self.topic_prefix, topic_name)
        return self.kc.topics[full_name.encode("ascii")]\
            .get_producer(use_rdkafka=True)

    def produce_alert(self, alert):
        self.alert_t.produce(repr(alert))

    def produce_error(self, error):
        self.error_t.produce(repr(error))
