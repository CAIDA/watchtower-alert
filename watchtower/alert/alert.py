import json


class Alert:

    LEVELS = ['critical', 'warning', 'normal', 'error']

    def __init__(self, name, level, time, expression, history_expression,
                 method, violations=None):
        self.name = name
        self.level = level
        self.time = time
        self.expression = expression
        self.history_expression = history_expression
        self.method = method
        self.violations = violations

    def __repr__(self):
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_str):
        obj = json.loads(json_str)
        # convert violations to objects
        obj['violations'] = [Violation(**viol) for viol in obj['violations']]
        return Alert(**obj)

    def as_dict(self):
        return {
            'name': self.name,
            'level': self.level,
            'time': self.time,
            'expression': self.expression,
            'history_expression': self.history_expression,
            'method': self.method,
            'violations': [v.as_dict() for v in self.violations],
        }

    @property
    def name(self):
        return self.name

    @name.setter
    def name(self, v):
        self.name = v

    @property
    def level(self):
        return self.level

    @level.setter
    def level(self, v):
        if v not in self.LEVELS:
            raise TypeError('Alert level must be one of %s' % self.LEVELS)
        self.level = v

    @property
    def time(self):
        return self.time

    @time.setter
    def time(self, v):
        if not isinstance(v, int):
            raise TypeError('Alert time must be an integer (UTC epoch time)')
        self.time = v

    @property
    def expression(self):
        return self.expression

    @expression.setter
    def expression(self, v):
        self.expression = v

    @property
    def history_expression(self):
        return self.history_expression

    @history_expression.setter
    def history_expression(self, v):
        self.history_expression = v

    @property
    def method(self):
        return self.method

    @method.setter
    def method(self, v):
        self.method = v

    @property
    def violations(self):
        return self.violations

    @violations.setter
    def violations(self, v):
        if not all(isinstance(viol, Violation) for viol in v):
            raise TypeError('Alert violations must be of type Violation')
        self.violations = v


class Violation:

    def __init__(self, expression, condition, value, history_value, history):
        self.expression = expression
        self.condition = condition
        self.value = value
        self.history_value = history_value
        self.history = history

    def __repr__(self):
        return json.dumps(self.as_dict())

    def as_dict(self):
        return {
            'expression': self.expression,
            'condition': self.condition,
            'value': self.value,
            'history_value': self.history_value,
            'history': self.history,
        }

    @property
    def expression(self):
        return self.expression

    @expression.setter
    def expression(self, v):
        self.expression = v
        
    @property
    def condition(self):
        return self.condition

    @condition.setter
    def condition(self, v):
        self.condition = v
        
    @property
    def value(self):
        return self.value

    @value.setter
    def value(self, v):
        self.value = v

    @property
    def history_value(self):
        return self.history_value

    @history_value.setter
    def history_value(self, v):
        self.history_value = v
        
    @property
    def history(self):
        return self.history

    @history.setter
    def history(self, v):
        if not isinstance(v, list):
            raise TypeError('Violation history must be a list')
        self.history = v


class Error:

    def __init__(self, name, type, time, expression, history_expression, message):
        self.name = name
        self.type = type
        self.time = time
        self.expression = expression
        self.history_expression = history_expression
        self.message = message

    def __repr__(self):
        return json.dumps(self.as_dict())

    def as_dict(self):
        return {
            'name': self.name,
            'type': self.type,
            'time': self.time,
            'expression': self.expression,
            'history_expression': self.history_expression,
            'message': self.message,
        }

    @classmethod
    def from_json(cls, json_str):
        obj = json.loads(json_str)
        return Error(**obj)

    @property
    def name(self):
        return self.name

    @name.setter
    def name(self, v):
        self.name = v

    @property
    def type(self):
        return self.type

    @type.setter
    def type(self, v):
        self.type = v

    @property
    def time(self):
        return self.time

    @time.setter
    def time(self, v):
        if not isinstance(v, int):
            raise TypeError('Error time must be an integer (UTC epoch time)')
        self.time = v

    @property
    def expression(self):
        return self.expression

    @expression.setter
    def expression(self, v):
        self.expression = v

    @property
    def history_expression(self):
        return self.history_expression

    @history_expression.setter
    def history_expression(self, v):
        self.history_expression = v
        
    @property
    def message(self):
        return self.message

    @message.setter
    def message(self, v):
        self.message = v
