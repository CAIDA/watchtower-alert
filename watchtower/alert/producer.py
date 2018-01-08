import confluent_kafka


class Producer:

    def __init__(self,
                 brokers='localhost:9092',
                 topic_prefix='watchtower',
                 alert_topic='alerts',
                 error_topic='errors'):
        self.topic_prefix = topic_prefix

        self.alert_topic = self._build_topic(alert_topic)
        self.error_topic = self._build_topic(error_topic)

        # connect to kafka
        self.kc = confluent_kafka.Producer({
            'bootstrap.servers': brokers
        })

    def _build_topic(self, topic_name):
        return "%s-%s" % (self.topic_prefix, topic_name)

    def produce_alert(self, alert):
        self.kc.produce(self.alert_topic, repr(alert))

    def produce_error(self, error):
        self.kc.produce(self.error_topic, repr(error))
