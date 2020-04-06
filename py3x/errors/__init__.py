from ..utils import IRANGE, SRE_Pattern, UTC_OFFSET, die, qw
from datetime import date, datetime
from time import mktime
import sys


class Error:
    __slots__ = qw('name msg args')

    def __call__(self, *args):
        n = self.args
        isinstance(n, int) or die(TypeError('%r is not callable' % self))
        len(args) == n or die.n_args(self.name + '()', n, 'p', len(args))
        e = object.__new__(self.__class__)
        osa = object.__setattr__
        osa(e, 'name', self.name)
        osa(e, 'msg', self.msg % args)
        osa(e, 'args', args)
        return e

    def __eq__(self, v):
        return isinstance(v, Error) and v.msg == self.msg

    def __hash__(self):
        return hash(self.msg)

    def __init__(self, msg):
        osa = object.__setattr__
        osa(self, 'msg', msg)
        osa(self, 'args', msg.count('%s') or ())

    def __repr__(self):
        r = f'errors.{self.name}'
        return r if not self.args or isinstance(self.args, int) else \
            f'{r}({[*self.args].__repr__()[1:-1]})'

    def __setattr__(self, k, v):
        die.ro_attr(type(self), k)

    def as_json(self):
        return self.msg


BAD_CHARS = Error('contains bad character(s) %s')
BAD_FORMAT = Error('is not valid format')
BLANK = Error("can't be blank")
CONFLICT = Error('saving failed due to another update')
INVALID = Error('is invalid')
TAKEN = Error('has already been taken')
TOO_EARLY = Error('must be later than or equal to %s')
TOO_GREAT = Error('must be less than or equal to %s')
TOO_LATE = Error('must be earlier than or equal to %s')
TOO_LITTLE = Error('must be greater than or equal to %s')
TOO_LONG = Error('is too long (maximum is %s < %s characters)')
TOO_SHORT = Error('is too short (minimum is %s > %s characters)')
UNSELECTED = Error('is not selected')


class Errors(dict):
    __slots__ = ()
    _BAD_CHARS_LIMIT = 3
    _WS = {
        "\t": '[TAB]',
        "\n": '[LF]',
        "\r": '[CR]',
        ' ': '[SP]',
        '　': '[MULTI-BYTE SP]',
    }

    @classmethod
    def __init_subclass__(cls):
        osa = object.__setattr__
        for k, v in sys._getframe(1).f_locals.items():
            if isinstance(v, Error):
                osa(v, 'name', k)
                setattr(cls, k, v)

    def __init__(self, d=None):
        if d is not None:
            for k, es in d.items():
                for e in es:
                    self.add(k, e)

    def __repr__(self):
        return f'{self.__class__.__name__}({super().__repr__() or ""})'

    def add(self, k, x):
        isinstance(x, Error) and isinstance(x.args, tuple) or die(TypeError(x))
        es = super().get(k, None)
        if es is None:
            es = self[k] = []
        m = x.msg
        any(e.msg == m for e in es) or es.append(x)
        return self

    def clear(self):
        super().clear()
        return self

    def find(self, *args):
        if len(args) == 2:
            es = self.get(args[0])
            x = args[1]
        elif len(args) == 1:
            es = (e for es in self.values() for e in es)
            x = args[0]
        else:
            die.n_args('find()', '1 or 2', 'p', len(args))

        isinstance(x, Error) or die.type(x, Error)
        k = 'name' if isinstance(x.args, int) else 'msg'
        v = getattr(x, k)
        for e in es:
            if getattr(e, k) == v:
                return e

    def get(self, k, x=()):
        return super().get(k, x)

    def validate(self, key, val, *, type=None, cast=None, nn=None, **kw):
        vt = type is None or val is None or isinstance(val, type)
        if not vt and cast:
            val = cast[0](val, type, *cast[1:]) \
                if isinstance(cast, tuple) else cast(val, type)
            vt = val is None or isinstance(val, type)
        if not vt:
            return self.validate_type(key, val, type)
        if not (val or val == 0):
            return self.validate_nn(key, val, nn) if nn else True

        if type is int and 'range' not in kw:
            kw['range'] = IRANGE[4]
        res = True
        for k, v in kw.items():
            res = getattr(self, 'validate_' + k)(key, val, v) and res
        return res

    def validate_chars(self, k, v, o: SRE_Pattern):
        v = o.sub('', v)
        if not v:
            return True

        cs = {}
        for c in v:
            if c not in cs:
                if len(cs) == self._BAD_CHARS_LIMIT:
                    cs[c] = '...'
                    break
                cs[c] = self._WS.get(c) or c.__repr__()
        self.add(k, self.BAD_CHARS(', '.join(cs.values())))

    def validate_choice(self, k, v, o):
        if v in o:
            return True
        self.add(k, self.INVALID)

    def validate_choices(self, k, vs, o):
        if all(v in o for v in vs) and len({*vs}) == len(vs):
            return True
        self.add(k, self.INVALID)

    def validate_len(self, k, v, o):
        n = len(v)
        if o[1] is not None and n > o[1]:
            self.add(k, self.TOO_LONG(o[1], n))
        elif o[0] is not None and n < o[0]:
            self.add(k, self.TOO_SHORT(o[0], n))
        else:
            return True

    def validate_nn(self, k, v, o):
        if v or v == 0:
            return True
        self.add(k, o if o is self.UNSELECTED else self.BLANK)

    def validate_range(self, k, v, o):
        if isinstance(v, int):
            f, t = o
            if f is not None and v < f:
                self.add(k, self.TOO_LITTLE(f))
            elif t is not None and v > t:
                self.add(k, self.TOO_GREAT(t))
            else:
                return True
        elif isinstance(v, datetime):
            f, t = (int(mktime(e.timetuple())) if isinstance(e, date) else e
                    for e in o)
            e = int(v.timestamp())
            if f is not None and e < f:
                e, x = self.TOO_EARLY, f
            elif t is not None and e > t:
                e, x = self.TOO_LATE, t
            else:
                return True
            self.add(k, e(datetime.fromtimestamp(x).strftime(
                '%F %T' if x % 60 else '%F %H:%M')))
        elif isinstance(v, date):
            f, t = (int(mktime(e.timetuple())) if isinstance(e, date) else e
                    for e in o)
            e = int(mktime(v.timetuple()))
            if f is not None and e < f:
                e, x = self.TOO_EARLY, f + ((f + UTC_OFFSET) % 86400 and 86400)
            elif t is not None and e > t:
                e, x = self.TOO_LATE, t
            else:
                return True
            self.add(k, e(date.fromtimestamp(x).isoformat()))

    def validate_re(self, k, v, o: SRE_Pattern):
        if o.search(v):
            return True
        self.add(k, self.BAD_FORMAT)

    def validate_type(self, k, v, o):
        if v is None or isinstance(v, o):
            return True
        self.add(k, self.BAD_FORMAT)


Errors.__init_subclass__()
