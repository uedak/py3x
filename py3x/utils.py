from collections.abc import Hashable
from datetime import date, datetime, timedelta, timezone
from types import MethodType
import json
import logging
import os
import re
import sys
import time


_1970_01_01 = date(1970, 1, 1)
IRANGE = {i: (-j, j - 1) for i, j in (
          (i, 1 << (i * 8) - 1) for i in (1 << i for i in range(4)))}
IRANGE_U = {k: (0, v[1] - v[0]) for k, v in IRANGE.items()}  # unsigned
SRE_Pattern = type(re.compile('.'))
UTC_OFFSET = -time.timezone
TZ = timezone(timedelta(seconds=UTC_OFFSET), time.tzname[0])


class _NX:
    pass


class AsJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        f = getattr(obj, 'as_json', None)
        return f() if isinstance(f, MethodType) else super().default(obj)


class cached_property:
    def __init__(self, fn, nm=None):
        self.fget = fn
        self.name = nm or fn.__name__

    def __get__(self, obj, cls=None):
        v = obj.__dict__[self.name] = self.fget(obj)
        return v


class cached_class_property(cached_property):
    def __get__(self, obj, cls):
        v = self.fget(cls)
        setattr(cls, self.name, v)
        return v


class caplog(list):
    def __enter__(self):
        h = self.h = logging.Handler()
        h.emit = lambda r: self.append(r)
        logging.getLogger().addHandler(h)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.getLogger().removeHandler(self.h)


class die:
    BAD_KW = '%s got an unexpected keyword argument %r'
    NO_KW = '%s missing required keyword argument: %r'

    @classmethod
    def __init_subclass__(cls):
        for k, v in tuple(cls.__dict__.items()):
            if isinstance(v, str) and k.upper() == k:
                setattr(cls, k.lower(), cls._f(v))

    @classmethod
    def _f(cls, v):
        def f(*args):
            raise TypeError(v % args)
        return f

    @classmethod
    def n_args(cls, m, n, t, g):
        t = 'positional' if t == 'p' else 'keyword' if t == 'k' else \
            'positional/keyword' if t == 'p/k' else cls(t)
        raise TypeError(
            '%s takes %s %s argument%s but %d %s given' %
            (m, n, t, '' if n == 1 else 's', g, 'was' if g == 1 else 'were'))

    @classmethod
    def no_attr(cls, t, k):
        raise AttributeError(
            '%r object has no attribute %r' % (t.__name__, k))

    @classmethod
    def ro_attr(cls, t, k):
        raise AttributeError(
            '%r object attribute %r is read-only' % (t.__name__, k))

    @classmethod
    def type(cls, x, *ts, kw=''):
        raise TypeError('%smust be %s, not %s' % (
            kw and f"keyword argument '{kw}' ",
            ' or '.join('None' if t is None else (t.__name__) for t in ts),
            type(x).__name__))

    def __init__(self, *args):
        e = args[0] if len(args) == 1 and isinstance(args[0], Exception) else \
            RuntimeError(*args)
        raise e


die.__init_subclass__()


def include(cls):
    locals = sys._getframe(1).f_locals
    nxd = _NX.__dict__
    for k, v in cls.__dict__.items():
        if k not in nxd:
            locals[k] = v


def qw(s):
    return tuple(s.strip().split())


def repr_kw(kw, max=None):
    if max is None:
        max = Util.TERM_W
    m = Util.RE_KW.fullmatch
    rs = []
    for k, v in kw.items():
        f = '%s=%s' if isinstance(k, str) and m(k) else '**{%r: %s}'
        v = '%r' % (v,)
        rs.append(f % (k, f'{v[:max]}...' if max and len(v) > max else v))
    if max and sum(len(r) + 2 for r in rs) >= max:
        return "\n  " + ",\n  ".join(rs) + "\n"
    return ', '.join(rs)


def try_(fn, x=None):
    try:
        return fn()
    except Exception:
        return x


def warn(msg='', exc=None, level=logging.WARN, **kw):
    if isinstance(msg, Exception):
        msg, exc = '', msg
    if exc:
        kw['exc_info'] = (exc.__class__, exc, exc.__traceback__)
    elif 'stack_info' not in kw:
        kw['stack_info'] = True

    pkg = sys._getframe(1).f_globals['__name__']
    lgr = logging.Logger(pkg)
    lgr.parent = logging.getLogger()
    lgr._log(level, msg, (), **kw)


def wrap_displayhook():  # pragma: no cover
    import builtins
    _displayhook_ = sys.displayhook
    vb = {'verbose': bool}

    def displayhook(v):
        r = getattr(v, '__repr__', None)
        a = r and getattr(r, '__annotations__', None)
        if a == vb and isinstance(r, MethodType):
            print(r(True))
            builtins._ = v
        else:
            _displayhook_(v)

    sys.displayhook = displayhook


class DailyLogHandler(logging.FileHandler):
    def __init__(self, *args, **kw):
        self._offset = None
        super().__init__(*args, **kw)

    def _open(self):
        ymd = Util.today()
        self.rolloverAt = (ymd + 1).epoch()
        path = f'{self.baseFilename}.{ymd}'
        fh = open(path, self.mode, encoding=self.encoding)
        if not self._offset:
            self._offset = (ymd, os.path.getsize(path))
        return fh

    def emit(self, record):
        if record.created >= self.rolloverAt:
            if self.stream is not None:
                self.stream.flush()
                self.stream.close()
            self.stream = self._open()
        super().emit(record)

    def readlines(self):
        if self._offset:
            ymd, pos = self._offset
            tdy = Util.today()
            while ymd <= tdy:
                path = f'{self.baseFilename}.{ymd}'
                ymd += 1
                if os.path.exists(path):
                    with open(path) as fh:
                        pos and fh.seek(pos)
                        pos = 0
                        for ln in fh:
                            yield ln


class _DT:
    @classmethod
    def _delegate(cls, k):
        setattr(cls, k, property(lambda self: getattr(self._dt_(), k)))

    @classmethod
    def _delegate_from(cls, src, k, classmethod=False):
        to_dt = getattr(src, k)
        if classmethod:
            def f(cls, *args, **kw):
                return cls.from_dt(to_dt(*args, **kw))
            setattr(cls, k, classmethod(f))
        else:
            def f(self, *args, **kw):
                return self.from_dt(to_dt(self._dt_(), *args, **kw))
            setattr(cls, k, f)
        f.__qualname__ = f'{src.__name__}.{k}'

    @classmethod
    def _dt2ts(cls, dt):
        isinstance(dt, date) or die(TypeError(dt))
        ts = getattr(dt, 'timestamp', None)
        return ts() if ts else (86400 * (dt - _1970_01_01).days - UTC_OFFSET)

    @classmethod
    def from_db(cls, s, csr=None):
        if s is not None:
            self = object.__new__(cls)
            self._s = s
            self._dt = True
            return self

    @classmethod
    def from_dt(cls, dt):
        self = object.__new__(cls)
        self._s = None
        self._dt = dt
        return self

    def __add__(self, d):
        return self.from_dt(self._dt_() + (
            self.resolution * d if isinstance(d, int) else d))

    def __eq__(self, x):
        xs = isinstance(x, self.CLS) and x._s
        return xs == self._s if xs and self._s else x == self._dt_()

    def __ge__(self, x):
        return self.timestamp() >= self._dt2ts(x)

    def __gt__(self, x):
        return self.timestamp() > self._dt2ts(x)

    def __hash__(self):
        return hash(self._dt_())

    def __le__(self, x):
        return self.timestamp() <= self._dt2ts(x)

    def __lt__(self, x):
        return self.timestamp() < self._dt2ts(x)

    def __repr__(self):
        return '%s(%r)' % (self.CLS.__name__, self.__str__())

    def __rsub__(self, d):
        return d - self._dt_()

    def __sub__(self, d):
        return self.__add__(-d) if isinstance(d, int) else (self._dt_() - d)

    def add(self, *, years=0, months=0, day=1):
        dt = self._dt_()
        m = dt.month + months - 1
        return self.from_dt(dt.replace(
            year=dt.year + years + m // 12, month=m % 12 + 1, day=day))

    @property
    def day(self):
        s = self._s
        return int(s[8:10]) if s else self._dt.day

    def is_valid(self):
        return bool(try_(self._dt_))

    @property
    def month(self):
        s = self._s
        return int(s[5:7]) if s else self._dt.month

    @property
    def year(self):
        s = self._s
        return int(s[:4]) if s else self._dt.year


for k in qw('ctime isocalendar isoformat isoweekday strftime timetuple '
            'toordinal weekday'):
    _DT._delegate(k)


class Date:
    include(_DT)
    __class__ = date  # isinstance of date
    __slots__ = ('_s', '_dt')
    LEN = 10
    RE_DATE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
    resolution = date.resolution

    def __init__(self, s):
        self._s = s
        self._dt = None

    def __str__(self):
        s = self._s
        if s is None:
            s = self._s = self._dt.isoformat()
        return s

    def _dt_(self):
        dt = self._dt
        if dt is None:
            self._dt = False
            dt = self._dt = bool(self.RE_DATE.fullmatch(self._s))
        if dt is True:
            self._dt = False
            s = self._s
            dt = self._dt = date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        return dt or die(TypeError('invalid Date: %r' % (self._s,)))

    _date = _dt_
    as_json = __str__

    def datetime(self):
        if self._dt is True:
            return self.DateTime.from_db(self._s + ' 00:00:00')
        dt = self._dt_()
        return self.DateTime.from_dt(datetime(dt.year, dt.month, dt.day))

    def epoch(self):
        return 86400 * (self._dt_() - _1970_01_01).days - UTC_OFFSET

    timestamp = epoch


Date.CLS = Date
Date.max = Date('9999-12-31')
Date.min = Date('0001-01-01')
for k in qw('fromordinal fromtimestamp today'):
    Date._delegate_from(date, k, classmethod)
Date._delegate_from(date, 'replace')


class DateTime:
    include(_DT)
    __class__ = datetime  # isinstance of datetime
    __slots__ = ('_s', '_dt')
    Date = Date
    LEN = 19
    LEN2SFX = {10: ' 00:00:00', 13: ':00:00', 16: ':00'}
    RE_DATETIME = re.compile(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
        r'(?:\.[0-9]{1,6})?([+-][0-9]{2}:?[0-9]{2}|Z)?')
    resolution = timedelta(seconds=1)

    @classmethod
    def combine(cls, d, t, tz=None):
        return cls.from_dt(datetime.combine(
            d._dt_() if isinstance(d, Date) else d, t, tz))

    @classmethod
    def now(cls, tz=None):
        return cls.from_dt(datetime.fromtimestamp(int(time.time()), tz))

    @classmethod
    def today(cls):
        return cls.now()

    def __init__(self, s):
        sl = isinstance(s, str) and len(s)
        sfx = sl and sl < 19 and self.LEN2SFX.get(sl)
        self._s = s + sfx if sfx else s
        self._dt = None

    def __str__(self):
        s = self._s
        if s is None:
            s = self._s = self._dt.isoformat(' ')
        return s

    def _dt_(self):
        dt = self._dt
        tz = None
        if dt is None:
            self._dt = False
            m = self.RE_DATETIME.fullmatch(self._s)
            tz = m and m.group(1)
            dt = self._dt = bool(m)
        if dt is True:
            self._dt = False
            s = self._s
            if tz:
                s = s[:-len(tz)]
                o = 0 if tz == 'Z' else (-1 if tz[0] == '-' else 1) * (
                    3600 * int(tz[1:3]) + 60 * int(tz[-2:]))
                tz = timezone(timedelta(seconds=o))  # TZ if o == UTC_OFFSET
            dt = self._dt = datetime(
                int(s[:4]), int(s[5:7]), int(s[8:10]),
                int(s[11:13]), int(s[14:16]), int(s[17:19]),
                int(s[20:] + '0' * (26 - len(s))) if len(s) > 19 else 0, tz)
        return dt or die(TypeError('invalid DateTime: %r' % (self._s,)))

    _datetime = _dt_
    as_json = __str__

    def date(self):
        return self.Date.from_db(self._s[:10]) if self._dt is True else \
            self.Date.from_dt(self._dt_().date())

    def epoch(self):
        return int(self.timestamp())

    @property
    def hour(self):
        s = self._s
        return int(s[11:13]) if s else self._dt.hour

    @property
    def minute(self):
        s = self._s
        return int(s[14:16]) if s else self._dt.minute

    @property
    def second(self):
        s = self._s
        return int(s[17:19]) if s else self._dt.second


DateTime.CLS = Date.DateTime = DateTime
DateTime.max = DateTime('9999-12-31 23:59:59+00:00')
DateTime.min = DateTime('0001-01-01 00:00:00+00:00')
for k in qw('fromordinal fromtimestamp strptime utcfromtimestamp utcnow'):
    DateTime._delegate_from(datetime, k, classmethod)
for k in qw('astimezone replace'):
    DateTime._delegate_from(datetime, k)
for k in qw('dst fold microsecond time timestamp timetz tzinfo tzname '
            'utcoffset utctimetuple'):
    DateTime._delegate(k)


class DebugStreamHandler(logging.StreamHandler):  # pragma: no cover
    def format(self, record):
        msg = super().format(record)
        return msg + "\n" if record.levelno <= logging.INFO else \
            f"\x1B[31m{msg}\x1B[0m\n"


class _FormUtil:
    @classmethod
    def getlist_from(cls, f, nm):
        g = getattr(f, 'getlist', None)
        return (cls.val2str(f.get(nm)),) if g is None else g(nm)

    @classmethod
    def input_tag_attrs(cls, f, nm, attrs, chk=None):
        attrs['name'] = nm
        if chk:
            attrs['type'] = chk
            v = attrs['value'] = cls.val2str(attrs['value'])
            c = attrs.get('checked', _NX)
            if v in cls.getlist_from(f, nm) if c is _NX else c:
                attrs['checked'] = '1'
            elif 'checked' in attrs:
                del attrs['checked']
        elif 'value' not in attrs:
            vs = cls.getlist_from(f, nm)
            attrs['value'] = vs[0] if vs else ''
        return attrs

    @classmethod
    def select_tag_options(cls, f, nm, opts):
        v2s = cls.val2str
        ss = set(cls.getlist_from(f, nm))
        for k, v in opts.items():
            k = v2s(k)
            yield k, k in ss, v

    @classmethod
    def str2bool(cls, s, strict=None):
        return True if s in ('1', 1) else False if s in ('0', 0) else s

    @classmethod
    def str2date(cls, s, strict=None):
        return cls.is_date_str(s) or s

    @classmethod
    def str2datetime(cls, s, strict=None):
        return cls.is_datetime_str(s, strict) or s

    @classmethod
    def str2int(cls, s, strict=None):
        v = s
        if strict is False and isinstance(v, str):
            v = v.replace(',', '')
        return int(v) if cls.is_int_str(v) else s

    STR2VAL = {
        bool: 'str2bool',
        Date: 'str2date',
        DateTime: 'str2datetime',
        int: 'str2int',
    }

    @classmethod
    def str2val(cls, s, t, strict=True):
        m = cls.STR2VAL[t]
        return None if s == '' else getattr(cls, m)(s, strict)

    @classmethod
    def val2str(cls, v):
        return '' if v is None else \
            ('1' if v else '0') if isinstance(v, bool) else str(v)


class Util:
    include(_FormUtil)
    Date = Date
    DateTime = DateTime
    HDR2RCPT = {'to', 'cc', 'bcc'}
    LogHandler = DailyLogHandler
    RE_ADDR = re.compile(r'(.*)<([!-;=?-~]+)>')
    RE_CHARS = re.compile(r'[^\x00-\x08\x0b-\x1f]+')
    RE_CHARS_INLINE = re.compile(r'[^\x00-\x08\x0a-\x1f]+')
    RE_CRLF_OR_CR = re.compile(r"\r\n?")
    RE_EMAIL = re.compile(
        r'[0-9A-Za-z][\+\-./0-9A-Za-z_^\?]*\@([0-9A-Za-z\-]+\.)+[A-Za-z]{2,}')
    RE_INT = re.compile(r'[+-]?[0-9]+')
    RE_KW = re.compile(r'[A-Za-z_][0-9A-Za-z_]*')
    RE_URL_ENCODE = re.compile(r'[^*\-.0-9@A-Z_a-z]')

    @cached_class_property
    def TERM_W(cls):
        return try_(lambda: os.get_terminal_size().columns, 80)

    @classmethod
    def gz6(cls, bin):
        from gzip import GzipFile
        from io import BytesIO
        buf = BytesIO()
        with GzipFile(fileobj=buf, mode='wb', compresslevel=6, mtime=0) as f:
            f.write(bin)
        return buf.getvalue()

    @classmethod
    def init_logger(cls, path, is_debug=False, more=None):
        def emit(rec):
            msg = rec.getMessage()
            ei = rec.exc_info
            tb = ''
            if ei:
                em = f'{ei[0].__name__}: {ei[1]}'
                msg = em if msg == '' else ': '.join((msg, em))
                tb = ei[2]
            elif rec.stack_info:
                tb = rec.stack_info
                if tb.startswith("Stack (most recent call last):\n"):
                    tb = tb[31:]

            rec.msg = msg.rstrip()
            tb = rec.exc_text = tb and cls.traceback_str(tb).strip()\
                .replace("\n  ", "\n") or ''
            if more:
                rec.exc_text += (tb and "\n") + (try_(more) or '')
            rec.args = rec.stack_info = None

        logging.captureWarnings(True)
        lgr = logging.getLogger()
        h = logging.Handler()
        h.emit = emit
        lgr.addHandler(h)

        lgh = cls.LogHandler(path)
        cls.init_logger_format(lgh)
        lgr.addHandler(lgh)

        is_debug and lgr.addHandler(DebugStreamHandler())

    @classmethod
    def init_logger_format(cls, lgh):
        class IndentFormatter(logging.Formatter):
            def format(self, r):
                return super().format(r).replace("\n", "\n\t") + "\n"

        lgh.setFormatter(IndentFormatter(
            "%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s"))

    @classmethod
    def is_bool_str(cls, s):
        return s in ('0', '1')

    @classmethod
    def is_date_str(cls, s):
        d = cls.Date(s)
        return d.is_valid() and d

    @classmethod
    def is_datetime_str(cls, s, strict=True):
        dt = cls.DateTime(s)
        return dt.is_valid() and (not strict or len(s) == dt.LEN) and dt

    @classmethod
    def is_email_str(cls, s):
        return isinstance(s, str) and cls.RE_EMAIL.fullmatch(s)

    @classmethod
    def is_int_str(cls, s):
        return isinstance(s, str) and cls.RE_INT.fullmatch(s)

    json_dumps = AsJsonEncoder(ensure_ascii=False).encode

    json_loads = json.loads

    @cached_class_property
    def now(cls):
        return cls.DateTime.now

    @classmethod
    def now_epoch(cls):
        return int(time.time())

    @classmethod
    def rm_rf(cls, path):  # shutil is heavy
        if os.path.isdir(path) and not os.path.islink(path):
            for n in os.listdir(path):
                cls.rm_rf(os.path.join(path, n))
            os.rmdir(path)
        else:
            os.unlink(path)

    @classmethod
    def sendmail_(cls, hdr, body, atts):
        from email import encoders
        from email.header import Header
        from email.mime.base import MIMEBase
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        import mimetypes

        sender = None
        rcpt = set()
        msg = MIMEMultipart()
        msg['Date'] = cls.now().astimezone(TZ).strftime('%a, %d %b %Y %T %z')
        re_addr = cls.RE_ADDR.fullmatch
        for k, v in hdr.items():
            lk = k.lower()
            if lk == 'from':
                m = re_addr(v)
                if m:
                    sender = m.group(2)
                    msg[k] = cls.utf8_mime_addr(m)
                else:
                    sender = msg[k] = v
            elif lk == 'reply-to':
                m = re_addr(v)
                msg[k] = cls.utf8_mime_addr(m) if m else v
            else:
                if lk in cls.HDR2RCPT:
                    for e in v.split(','):
                        rcpt.add(e.strip())
                if lk != 'bcc':
                    msg[k] = v
        rcpt and all(cls.is_email_str(e) for e in (sender, *rcpt)) or die(hdr)

        msg.attach(MIMEText(body, 'plain'))
        for fnm, bin in atts:
            m = mimetypes.guess_type(fnm)[0]
            m = MIMEBase(*(m or 'application/octet-stream').split('/'))
            m.set_payload(bin)
            encoders.encode_base64(m)
            m.add_header(
                'Content-Disposition', 'attachment',
                filename=Header(fnm).encode())
            msg.attach(m)
        return (sender, rcpt, msg)

    sleep = time.sleep

    @cached_class_property
    def today(cls):
        return cls.Date.today

    @classmethod
    def traceback_str(cls, tb):
        if not isinstance(tb, str):
            import traceback
            tb = ''.join(traceback.format_tb(tb))
        return tb

    @cached_class_property
    def url_encode(cls):
        tr = {' ': '+'}
        for i in range(256):
            tr[i] = tr[chr(i)] = '%%%02X' % i

        def f(m):
            c = m.group()
            return tr.get(c) or ''.join(tr[b] for b in c.encode())

        sub = cls.RE_URL_ENCODE.sub
        return lambda s: sub(f, s)

    @classmethod
    def utf8_mime_addr(cls, m):
        dnm = m.group(1).strip()
        if not dnm:
            return m.group(2)

        from email.header import Header, UTF8
        h = Header()
        h.append(dnm, UTF8)
        h.append(f' <{m.group(2)}>')
        return h.encode()


class XEnum:
    def __contains__(self, v):
        return v is not None and self.get(v) is v

    def __getattr__(self, k0):
        d = self.__dict__
        v = d.get(k0, _NX)
        if v is _NX:
            hs, kw = d['.init']
            xs = d['.xs'] = []
            v2x = d['.v2x'] = {}
            t0 = next(iter(kw.values()))
            if type(t0) is not tuple:
                t0 = (t0,)
                kw = {k: (t, k) for k, t in kw.items()}

            X = d['.X'] = type(self.__name__ or 'XEnum', (type(t0[0]),), {})
            X.__module__ = self.__module__
            X.__repr__ = lambda x: (
                f'{self.__name__}.{x.name}' if self.__name__ else x.name)
            X.__setattr__ = XEnum.__setattr__
            X.__str__ = lambda x: str(x.value)
            for k, t in kw.items():
                len(t) == len(hs) or die(TypeError(t))
                v = t[0]
                x = d[k] = v2x[v] = v2x[Util.val2str(v)] = X(v)
                x.__dict__.update(zip(('name', *hs), (k, *t)))
                xs.append(x)

            v = d.get(k0, _NX)
            v is _NX and die.no_attr(type(self), k0)
        return v

    def __getitem__(self, v):
        return self.__getattr__('.v2x')[v]

    def __init__(self, hs=('value', 'label'), **kw):
        kw or die.n_args('XEnum()', 'some', 'k', 0)
        d = self.__dict__
        d['__module__'] = __name__
        d['__name__'] = None
        d['.init'] = (hs, kw)

    def __iter__(self):
        return iter(self.__getattr__('.xs'))

    def __repr__(self):
        return self.__name__ or 'XEnum'

    def __setattr__(self, k, v):
        k in self.__dict__ or die.no_attr(type(self), k)
        k in ('__module__', '__name__') or die.ro_attr(type(self), k)
        super().__setattr__(k, v)

    def get(self, k, x=None):
        return x if not isinstance(k, Hashable) else \
            self.__getattr__('.v2x').get(k, x)

    def items(self, k='label'):
        return ((x.value, getattr(x, k)) for x in self.__getattr__('.xs'))
