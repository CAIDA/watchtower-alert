import logging
import requests
import sqlalchemy
import sqlalchemy.engine.url

from watchtower.alert.consumers import AbstractConsumer


class DatabaseConsumer(AbstractConsumer):

    CH_META_API = "https://charthouse-test.caida.org/data/meta/annotate"

    defaults = {
        'drivername': 'sqlite',
        'username': None,
        'password': None,
        'host': './watchtower-alerts.db',
        'port': None,
        'databasename': None,
        'engine_params': {},
        'table_prefix': 'watchtower',
        'alert_table_name': 'alert',
        'violation_table_name': 'violation',
        'violation_meta_table_name': 'violation_meta',
        'error_table_name': 'error',
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
                              sqlalchemy.ForeignKey(self.t_alert.c.id))
        )

        self.t_violation_meta = sqlalchemy.Table(
            self._table_name('violation_meta'),
            meta,
            sqlalchemy.Column('violation_id', sqlalchemy.Integer,
                              sqlalchemy.ForeignKey(self.t_violation.c.id)),
            sqlalchemy.Column('type', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('val', sqlalchemy.String, nullable=False),
            sqlalchemy.UniqueConstraint('violation_id', 'type')
        )

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

    def _insert_violations(self, alert_id, vdicts):
        metas = self._lookup_meta(vdicts)
        for v in vdicts:
            v['alert_id'] = alert_id
            v['history'] = str(v['history'])
            with self.engine.connect() as conn:
                ins = self.t_violation.insert().values(v)
                res = conn.execute(ins)
                [viol_id] = res.inserted_primary_key
                self._insert_violation_meta(viol_id, metas[v['expression']])

    def _lookup_meta(self, vdicts):
        # build a list of all expressions to query for
        expressions = [v['expression'] for v in vdicts]
        resp = requests.post(self.CH_META_API, {'expression[]': expressions})
        res = resp.json()
        if 'data' not in res:
            return []
        metas = {}
        for expression in expressions:
            metas[expression] = []
            if expression not in res['data']:
                continue
            for ann in res['data'][expression]['annotations']:
                if ann['type'] != 'meta':
                    continue
                if ann['attributes']['type'] == 'geo':
                    metas[expression].append(self._parse_geo_ann(ann))
                elif ann['attributes']['type'] == 'asn':
                    metas[expression].append({
                        'type': 'asn',
                        'val': ann['attributes']['asn']
                    })
        return metas

    @staticmethod
    def _parse_geo_ann(ann):
        type = ann['attributes']['nativeLevel']
        return {
            'type': type,
            'val': ann['attributes'][type]['id']
        }

    def _insert_violation_meta(self, viol_id, metas):
        if not metas or not len(metas):
            return
        for meta in metas:
            meta.update({'violation_id': viol_id})
        with self.engine.connect() as conn:
            ins = self.t_violation_meta.insert().values(metas)
            conn.execute(ins)

    def handle_error(self, error):
        logging.debug("DB consumer handling error")
        with self.engine.connect() as conn:
            try:
                ins = self.t_error.insert().values(**error.as_dict())
                conn.execute(ins)
            except sqlalchemy.exc.IntegrityError:
                logging.warn("Error insert failed (maybe it already exists?)")
