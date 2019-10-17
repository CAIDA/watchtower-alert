import logging
import slack

from . import AbstractConsumer


MSG_JSON = """{
	"blocks": [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*{name}*"
			},
            "accessory": {
				"type": "button",
				"text": {
					"type": "plain_text",
					"text": "Show in Dashboard",
					"emoji": true
				},
				"url": "https://ioda.caida.org/ioda/dashboard#view=inspect&entity={meta_type}/{meta_code}&lastView=overview"
			}
		},
		{
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Type:* {position}"
				},
				{
					"type": "mrkdwn",
					"text": "*{meta_type}*: {meta_code}"
				}
			]
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Current Value:* {actual}"
				},
				{
					"type": "mrkdwn",
					"text": "*Predicted Value:* {predicted}"
				},
				{
					"type": "mrkdwn",
					"text": "*Relative Drop:* {pct_drop}%"
				}
			]
		},
		{
			"type": "context",
			"elements": [
				{
					"type": "mrkdwn",
					"text": "{alert_time}"
				}
			]
		}
	]
}
"""


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

    def handle_alert(self, alert):
        alert.annotate_violations()
        for viol in alert.violations:
            rel_drop = None
            if viol.history_value is not None and viol.value is not None:
                rel_drop = (viol.history_value - viol.value) / viol.history_value * 100
            msg_str = MSG_JSON.format(
                name=alert.name,
                meta_type=viol.meta['meta_type'] if 'meta_type' in viol.meta else "",
                meta_code=viol.meta['meta_code'] if 'meta_code' in viol.meta else "",
                position="Back to Normal" if alert.level == 'normal' else "Outage Alert",
                actual=viol.value,
                predicted=viol.history_value,
                pct_drop=rel_drop,
                alert_time=viol.time
            )
            logging.info(msg_str)

    def handle_error(self, error):
        pass

    def handle_timer(self, now):
        pass