import logging
import slack
import time

from . import AbstractConsumer

class SlackConsumer(AbstractConsumer):

    defaults = {
        'api_token': None,
        'channel': None
    }

    def __init__(self, config):
        super(SlackConsumer, self).__init__(self.defaults)
        if config:
            self.config.update(config)
        self.channel = None
        self.client = None

    def start(self):
        self.channel = self.config['channel']
        self.client = slack.WebClient(token=self.config['api_token'])

    def _post(self, msg_blocks):
        retries = 5
        while retries < 0:
            try:
                self.client.chat_postMessage(
                    channel=self.channel,
                    blocks=msg_blocks)
            except slack.error.SlackApiError as e:
                if e.response['error'] == 'ratelimited':
                    retries -= 1
                    continue
                raise e

    @staticmethod
    def _build_dashboard_url(meta_type, meta_code, from_time, until_time):
        return "https://ioda.caida.org/ioda/dashboard#view=inspect" \
               "&entity=%s/%s&lastView=overview&from=%s&until=%s" % \
               (meta_type, meta_code, from_time, until_time)

    def _build_msg_blocks(self, name, meta_type, meta_code,
                          from_time, until_time, position,
                          actual, predicted, pct_drop, alert_time):
        predicted_str = "%d" % predicted if predicted is not None else "Unknown"
        pct_drop_str = "%.2f%%" % pct_drop if pct_drop is not None else "Unknown"
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*%s*" % name
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Show in Dashboard",
                        "emoji": True
                    },
                    "url": self._build_dashboard_url(meta_type, meta_code, from_time, until_time)
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "*Type:* %s" % position
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*%s*: %s" % (meta_type.title(), meta_code)
                    }
                ]
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "*Current Value:* %d" % actual
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Predicted Value:* %s" % predicted_str
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Relative Drop:* %s" % pct_drop_str
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "%s" % alert_time
                    }
                ]
            },
            {
                "type": "divider"
            },  # divider
        ]

    def handle_alert(self, alert):
        alert.annotate_violations()
        for viol in alert.violations:
            # TODO: move this logic inside build_msg method
            rel_drop = None
            if viol.history_value is not None and viol.value is not None:
                rel_drop = (viol.history_value - viol.value) / viol.history_value * 100
            msg_blocks = self._build_msg_blocks(
                name=alert.name,
                meta_type=viol.meta['meta_type'] if 'meta_type' in viol.meta else "",
                meta_code=viol.meta['meta_code'] if 'meta_code' in viol.meta else "",
                from_time=viol.time - 8*3600,
                until_time=viol.time + 8*3600,
                position="Outage End" if alert.level == 'normal' else "Outage Start",
                actual=viol.value,
                predicted=viol.history_value,
                pct_drop=rel_drop,
                alert_time=time.strftime('%m/%d/%Y %H:%M:%S UTC',  time.gmtime(viol.time))
            )
            self._post(msg_blocks)

    def handle_error(self, error):
        pass

    def handle_timer(self, now):
        pass