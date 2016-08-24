import logging
import _pytimeseries

from watchtower.alert.consumers import AbstractConsumer


class TimeseriesConsumer(AbstractConsumer):

    defaults = {
        'interval': 60,
        'backends': ['ascii'],
        'ascii-opts': "",
        'metric_prefix': 'projects.ioda.alerts',
        'level_leaf': 'alert_level'
    }

    level_values = {
        'normal': 0,
        'warning': 1,
        'critical': 2,
    }

    def __init__(self, config):
        super(TimeseriesConsumer, self).__init__(self.defaults)
        if config:
            self.config.update(config)

        # [alert.name] => 'int_start', 'last_time', 'kp'
        self.alert_state = {}
        self.ts = None
        self._init_ts()

    def _init_ts(self):
        logging.info("Initializing PyTimeseries")
        self.ts = _pytimeseries.Timeseries()
        for name in self.config['backends']:
            logging.info("Enabling timeseries backend '%s'" % name)
            be = self.ts.get_backend_by_name(name)
            if not be:
                logging.error("Could not enable TS backend %s" % name)
            opts = self.config[name+'-opts'] if name+'-opts' in self.config else ""
            self.ts.enable_backend(be, opts)

        logging.debug("Creating new Key Package")

    def handle_alert(self, alert):
        # get the state for this alert type
        if alert.name in self.alert_state:
            state = self.alert_state[alert.name]
        else:
            state = {
                'int_start': self.compute_interval_start(alert.time),
                'last_time': alert.time,
                'kp': self.ts.new_keypackage(reset=False)
            }
            self.alert_state[alert.name] = state

        self._maybe_flush_kp(state, alert.time)

        # we need meta, so make sure it is loaded
        alert.annotate_violations()
        for v in alert.violations:
            if not len(v.meta):
                continue
            if len(v.meta) > 1:
                raise NotImplementedError('Multi-meta violations not supported')
            key = self._build_key(alert, v)
            logging.debug("Key: %s" % key)
            idx = state['kp'].get_key(key)
            if idx is None:
                idx = state['kp'].add_key(key)
            state['kp'].set(idx, self.level_values[alert.level])

    def _build_key(self, alert, violation):
        # "projects.ioda.alerts.[ALERT-NAME].[META-FQID].alert_level
        return '.'\
            .join((self.config['metric_prefix'], alert.name,
                   violation.meta[0]['fqid'],
                   self.config['level_leaf']))\
            .replace(' ', '-').replace('/', 'slash').replace('(', '').replace(')', '')  # hax for now

    def _maybe_flush_kp(self, state, time):
        this_int_start = self.compute_interval_start(time)
        if time < state['last_time']:
            raise RuntimeError('Time is going backward! Time: %d Last Time: %d'
                               % (time, state['last_time']))
        state['last_time'] = time
        if not state['int_start'] or this_int_start <= state['int_start']:
            state['int_start'] = this_int_start
            return

        while state['int_start'] < this_int_start:
            state['kp'].flush(state['int_start'])
            state['int_start'] += self.config['interval']

    def compute_interval_start(self, time):
        return int(time / self.config['interval']) * self.config['interval']

    def handle_error(self, error):
        pass

