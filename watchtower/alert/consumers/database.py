import logging
import sqlalchemy
import sqlalchemy.engine.url

from watchtower.alert.consumers import AbstractConsumer


class DatabaseConsumer(AbstractConsumer):

    defaults = {
        'drivername': 'sqlite',
        'username': None,
        'password': None,
        'host': './watchtower-alerts.db',
        'port': None,
        'databasename': None,
        'engine_params': {},
        'table_prefix': None,
        'alert_table_name': 'watchtower_alert',
        'violation_table_name': 'watchtower_violation',
        'error_table_name': 'watchtower_error',
    }

    def __init__(self, config):
        super(DatabaseConsumer, self).__init__(self.defaults)
        if config:
            self.config.update(config)

        self._init_db()

    def _init_db(self):
        meta = sqlalchemy.MetaData()

        self.t_alert = sqlalchemy.Table(
            self._table_name('alert'),
            meta,
            sqlalchemy.Column('id', sqlalchemy.Integer,
                              sqlalchemy.Sequence('watchtower_alert_id_seq'),
                              primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('time', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column('level', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('method', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('expression', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('history_expression', sqlalchemy.String, nullable=False),
            sqlalchemy.UniqueConstraint('name', 'time', 'level', 'expression')
        )

        # Violations are all occurences of rule violations grouped by level
        # under an alert
        self.t_violation = sqlalchemy.Table(
            self._table_name('violation'),
            meta,
            sqlalchemy.Column('id', sqlalchemy.Integer,
                              sqlalchemy.Sequence('watchtower_violation_id_seq'),
                              primary_key=True),
            sqlalchemy.Column('expression', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('condition', sqlalchemy.String),
            sqlalchemy.Column('value', sqlalchemy.Float),
            sqlalchemy.Column('history_value', sqlalchemy.Float),
            sqlalchemy.Column('history', sqlalchemy.String),
            sqlalchemy.Column('alert_id', sqlalchemy.Integer,
                              sqlalchemy.ForeignKey(self.t_alert.c.id)))

        self.t_error = sqlalchemy.Table(
            self._table_name('error'),
            meta,
            sqlalchemy.Column('id', sqlalchemy.Integer,
                              sqlalchemy.Sequence('watchtower_error_id_seq'),
                              primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('time', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column('expression', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('history_expression', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('type', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('message', sqlalchemy.String, nullable=False),
            sqlalchemy.UniqueConstraint('name', 'time', 'expression', 'type',
                                        'message')
        )

        engine_options = [self.config[n] for n in (
            'drivername', 'username', 'password', 'host', 'port',
            'databasename')]
        if 'sqlite' in engine_options[0] and engine_options[3]:
            engine_options[3] = '/' + engine_options[3] # host

        self.url = str(sqlalchemy.engine.url.URL(*engine_options))

        # Its a little unsafe to log this since it may have a password:
        # logging.debug('Database engine url: %s', self.url)

        self.engine = sqlalchemy.create_engine(self.url,
                                               **self.config['engine_params'])
        meta.create_all(self.engine)

    def _table_name(self, table):
        suffix = self.config['%s_table_name' % table]
        return "%s_%s" % (self.config['table_prefix'], suffix) \
            if self.config['table_prefix'] else suffix

    def handle_alert(self, alert):
        logging.debug("DB consumer handling alert")
        with self.engine.connect() as conn:
            adict = alert.as_dict()
            violdict = adict.pop('violations')
            # dirty hax below. should do a select first
            try:
                ins = self.t_alert.insert().values(adict)
                res = conn.execute(ins)
                [alert_id] = res.inserted_primary_key
                self._insert_violations(alert_id, violdict)
            except sqlalchemy.exc.IntegrityError:
                logging.warn("Alert insert failed (maybe it already exists?)")

    def _insert_violations(self, alert_id, violations):
        for v in violations:
            v['alert_id'] = alert_id
            v['history'] = str(v['history'])
        with self.engine.connect() as conn:
            ins = self.t_violation.insert().values(violations)
            conn.execute(ins)

    def handle_error(self, error):
        logging.debug("DB consumer handling error")
        with self.engine.connect() as conn:
            try:
                ins = self.t_error.insert().values(**error.as_dict())
                conn.execute(ins)
            except sqlalchemy.exc.IntegrityError:
                logging.warn("Error insert failed (maybe it already exists?)")
