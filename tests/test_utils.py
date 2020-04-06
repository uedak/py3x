from datetime import date, datetime, timedelta, timezone
from py3x.utils import Date, DateTime, Util, XEnum, \
    cached_class_property, cached_property, caplog, die, qw, repr_kw, warn
import gzip
import logging
import os
import pytest
import time
import warnings


def test_cached_class_property():
    class Foo:
        CNT = [0]

        @cached_class_property
        def cnt(cls):
            cls.CNT[0] += 1
            return cls.CNT[0]

    assert Foo.CNT == [0]
    assert Foo.cnt == 1
    assert Foo.CNT == [1]
    assert Foo.cnt == 1
    assert Foo.CNT == [1]
    assert Foo().cnt == 1
    assert Foo.CNT == [1]


def test_cached_property():
    class Foo:
        CNT = [0]

        @cached_property
        def cnt(self):
            self.CNT[0] += 1
            return self.CNT[0]

    assert Foo.CNT == [0]

    f = Foo()
    assert f.cnt == 1
    assert Foo.CNT == [1]
    assert f.cnt == 1
    assert Foo.CNT == [1]

    f = Foo()
    assert f.cnt == 2
    assert Foo.CNT == [2]
    assert f.cnt == 2
    assert Foo.CNT == [2]

    f = Foo()
    f.cnt = 5
    assert f.cnt == 5
    assert Foo.CNT == [2]
    assert f.cnt == 5
    assert Foo.CNT == [2]


def test_caplog():
    with caplog() as logs:
        warn('?')
    assert len(logs) == 1
    assert logs[0].getMessage() == '?'


def test_repr_kw():
    assert repr_kw({}) == ''
    assert repr_kw({
        'a': '12345678',
        'A': '123456789',
        '-x': 1,
        '-y': b'1234567',
    }, max=10) == '''
  a='12345678',
  A='123456789...,
  **{'-x': 1},
  **{'-y': b'1234567'}
'''


def assert_eq(d, x):
    assert d == x
    assert x == d

    assert not d != x
    assert not x != d

    assert d >= x
    assert x >= d

    assert d <= x
    assert x <= d

    assert not d > x
    assert not x > d

    assert not d < x
    assert not x < d


def assert_gt(d, x):
    assert not d == x
    assert not x == d

    assert d != x
    assert x != d

    assert d >= x
    assert not x >= d

    assert not d <= x
    assert x <= d

    assert d > x
    assert not x > d

    assert not d < x
    assert x < d


def test_Date():
    d1 = Date('2018-12-01')
    assert Date.from_db('2018-12-01') == d1
    assert Date.fromtimestamp(d1.epoch()) == d1
    # assert Date.strptime('2018/12/01', '%Y/%m/%d') == d1

    assert d1 + 1 == d1 + timedelta(days=1) == Date('2018-12-02')
    assert d1 - 1 == d1 - timedelta(days=1) == Date('2018-11-30')

    assert_eq(d1, Date('2018-12-01'))
    assert d1 != DateTime('2018-12-01')
    assert_eq(d1, date(2018, 12, 1))
    assert d1 != datetime(2018, 12, 1)

    assert_gt(d1, Date('2018-11-30'))
    assert_gt(d1, DateTime('2018-11-30 23:59:59'))
    assert_gt(d1, date(2018, 11, 30))
    assert_gt(d1, datetime(2018, 11, 30, 23, 59, 59))

    assert {d1: d1}[Date('2018-12-01')] == d1
    assert str(d1) == '2018-12-01'
    assert d1.add(months=1, day=2) == Date('2019-01-02')
    assert d1.datetime() == DateTime('2018-12-01')
    assert d1.day == 1
    assert d1.month == 12
    assert d1.weekday() == date(2018, 12, 1).weekday()
    assert d1.year == 2018

    assert Date('2018-12-01').is_valid()
    assert not Date('2018/12/01').is_valid()

    assert Util.json_dumps([d1]) == '["2018-12-01"]'
    assert Date.from_db('2018-12-01').datetime() == \
        DateTime('2018-12-01 00:00:00')


def test_Date_compat():
    d0 = date(2019, 12, 4)
    d1 = Date('2019-12-04')
    assert d1 == d0
    assert d0 == d1
    assert str(d1) == str(d0) == '2019-12-04'

    assert isinstance(d1, Date)
    assert not isinstance(d1, DateTime)
    assert isinstance(d1, date)
    assert not isinstance(d1, datetime)
    assert len({d0, d1}) == 1

    with pytest.raises(AttributeError) as e:
        d1.xxx
    assert e.value.args == ("'Date' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        d1.yyy = 100
    assert e.value.args == ("'Date' object has no attribute 'yyy'",)

    d2 = Date('2019-12-06')
    dt0 = datetime(2019, 12, 4)
    dt2 = DateTime('2019-12-04 00:00:02')
    for x in (
        (d0,  d0,  dict(seconds=0)),
        (d2,  d0,  dict(days=2)),
        (dt0, d0,  None),
        (dt2, d0,  None),
        (d0,  d2,  dict(days=-2)),
        (d2,  d2,  dict(seconds=0)),
        (dt0, d2,  None),
        (dt2, d2,  None),
        (d0,  dt0, None),
        (d2,  dt0, None),
        (dt0, dt0, dict(seconds=0)),
        (dt2, dt0, dict(seconds=2)),
        (d0,  dt2, None),
        (d2,  dt2, None),
        (dt0, dt2, dict(days=-1, seconds=86398)),
        (dt2, dt2, dict(seconds=0)),
    ):
        # print(x)
        if x[2] is None:
            with pytest.raises(TypeError) as e:
                x[0] - x[1]
            assert e.value.args[0].startswith('unsupported operand')
        else:
            assert (x[0] - x[1]) == timedelta(**x[2])

    assert d1.ctime() == d0.ctime() == 'Wed Dec  4 00:00:00 2019'
    for m in (date.ctime, Date.ctime):
        with pytest.raises(TypeError):
            m()

    assert d1.day == d0.day == 4

    assert '%r' % Date.fromordinal(737397) == \
           '%r' % d1.fromordinal(737397) == "Date('2019-12-04')"
    assert '%r' % date.fromordinal(737397) == \
           '%r' % d0.fromordinal(737397) == 'datetime.date(2019, 12, 4)'

    e1 = d1.epoch()
    assert '%r' % Date.fromtimestamp(e1) == \
           '%r' % d1.fromtimestamp(e1) == "Date('2019-12-04')"
    assert '%r' % date.fromtimestamp(e1) == \
           '%r' % d0.fromtimestamp(e1) == 'datetime.date(2019, 12, 4)'

    assert d1.isocalendar() == d0.isocalendar() == (2019, 49, 3)
    for m in (Date.isocalendar, date.isocalendar):
        with pytest.raises(TypeError):
            m()

    assert d1.isoformat() == d0.isoformat() == '2019-12-04'
    for m in (Date.isoformat, date.isoformat):
        with pytest.raises(TypeError):
            m()

    assert d1.isoweekday() == d0.isoweekday() == 3
    for m in (Date.isoweekday, date.isoweekday):
        with pytest.raises(TypeError):
            m()

    assert '%r' % [d1.min, d1.max] == \
        "[Date('0001-01-01'), Date('9999-12-31')]"
    assert '%r' % [d0.min, d0.max] == \
        '[datetime.date(1, 1, 1), datetime.date(9999, 12, 31)]'
    assert d1.min == d0.min
    assert d0.max == d1.max

    assert d1.month == d0.month == 12

    assert '%r' % d1.replace(day=5) == "Date('2019-12-05')"
    assert '%r' % d0.replace(day=5) == 'datetime.date(2019, 12, 5)'
    for m in (Date.replace, date.replace):
        with pytest.raises(TypeError):
            m(day=5)

    assert d1.resolution == d0.resolution

    assert d1.strftime('%F') == d0.strftime('%F') == '2019-12-04'
    for m in (Date.strftime, date.strftime):
        with pytest.raises(TypeError):
            m('%F')

    assert d1.timetuple() == d0.timetuple()
    for m in (Date.timetuple, date.timetuple):
        with pytest.raises(TypeError):
            m()

    assert Date.today() == d1.today() == date.today() == d0.today()

    assert d1.toordinal() == d0.toordinal() == 737397
    for m in (Date.toordinal, date.toordinal):
        with pytest.raises(TypeError):
            m()

    assert d1.weekday() == d0.weekday() == 2
    for m in (Date.weekday, date.weekday):
        with pytest.raises(TypeError):
            m()

    assert d1.year == d0.year == 2019

    dt = DateTime.from_db('2020-02-01 00:00:00.000001')
    assert str(dt + 1) == '2020-02-01 00:00:01.000001'

    dt = DateTime.from_db('2020-02-01 00:00:00.1')
    assert str(dt + 1) == '2020-02-01 00:00:01.100000'


def test_DateTime():
    d1 = DateTime('2018-12-01')
    assert DateTime.from_db('2018-12-01 00:00:00') == d1
    assert DateTime.fromtimestamp(d1.epoch()) == d1
    assert DateTime.strptime('2018/12/01', '%Y/%m/%d') == d1

    ts1 = timedelta(seconds=1)
    assert d1 + 1 == d1 + ts1 == DateTime('2018-12-01 00:00:01')
    assert d1 - 1 == d1 - ts1 == DateTime('2018-11-30 23:59:59')

    assert d1 != Date('2018-12-01')
    assert_eq(d1, DateTime('2018-12-01'))
    assert d1 != date(2018, 12, 1)
    assert_eq(d1, datetime(2018, 12, 1))

    assert_gt(d1, Date('2018-11-30'))
    assert_gt(d1, DateTime('2018-11-30 23:59:59'))
    assert_gt(d1, date(2018, 11, 30))
    assert_gt(d1, datetime(2018, 11, 30, 23, 59, 59))

    assert {d1: d1}[DateTime('2018-12-01')] == d1
    assert str(d1) == '2018-12-01 00:00:00'
    assert d1.add(months=1, day=2) == DateTime('2019-01-02')
    assert d1.date() == Date('2018-12-01')
    assert d1.day == 1
    assert d1.month == 12
    assert d1.weekday() == date(2018, 12, 1).weekday()
    assert d1.year == 2018

    d2 = d1 - 2
    assert d2.hour == 23
    assert d2.minute == 59
    assert d2.second == 58
    assert d2.tzinfo is None  # == TZ
    assert DateTime('2018-12-01 00:00:00Z').tzinfo.utcoffset(None).seconds == 0

    assert DateTime('2018-12-01').is_valid()
    assert DateTime('2018-12-01 00').is_valid()
    assert DateTime('2018-12-01 00:00:00').is_valid()
    assert DateTime('2018-12-01 00:00:00Z').is_valid()
    assert DateTime('2018-12-01 00:00:00+0900').is_valid()
    assert DateTime('2018-12-01 00:00:00+09:00').is_valid()
    assert not DateTime('2018/12/01T00:00:00').is_valid()

    assert Util.json_dumps([d1]) == '["2018-12-01 00:00:00"]'
    assert DateTime.from_db('2018-12-01 00:00:00').date() == \
        Date('2018-12-01')
    assert DateTime.from_db('2018-12-01 00:00:00.000123').microsecond == 123


def test_DateTime_compat():
    dt0 = datetime(2019, 12, 4, 8, 59, 58)
    dt1 = DateTime('2019-12-04 08:59:58')
    JST = timezone(timedelta(hours=9))

    assert datetime(2019, 12, 4, 8, 59, 58) == dt1
    assert datetime(2019, 12, 4, 8, 59, 58, 1) != dt1

    assert not isinstance(dt1, Date)
    assert isinstance(dt1, DateTime)
    assert isinstance(dt1, date)
    assert isinstance(dt1, datetime)
    assert len({dt0, dt1}) == 1

    with pytest.raises(AttributeError) as e:
        dt1.xxx
    assert e.value.args == ("'DateTime' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        dt1.yyy = 100
    assert e.value.args == ("'DateTime' object has no attribute 'yyy'",)

    assert type(dt1.astimezone(JST)) is DateTime
    assert type(dt0.astimezone(JST)) is datetime
    assert dt1.astimezone(JST).hour == dt0.astimezone(JST).hour

    assert '%r' % datetime.combine(dt0.date(), dt0.time()) == \
        'datetime.datetime(2019, 12, 4, 8, 59, 58)'
    assert '%r' % DateTime.combine(dt1.date(), dt1.time()) == \
        "DateTime('2019-12-04 08:59:58')"

    assert dt1.ctime() == dt0.ctime() == 'Wed Dec  4 08:59:58 2019'

    assert '%r' % dt1.date() == "Date('2019-12-04')"
    assert '%r' % dt0.date() == 'datetime.date(2019, 12, 4)'
    for m in (DateTime.date, datetime.date):
        with pytest.raises(TypeError):
            m()

    assert dt1.day == dt0.day == 4

    assert dt1.dst() is dt0.dst() is None

    assert dt1.fold == dt0.fold == 0

    assert '%r' % DateTime.fromordinal(737397) == \
           '%r' % dt1.fromordinal(737397) == \
           "DateTime('2019-12-04 00:00:00')"
    assert '%r' % datetime.fromordinal(737397) == \
           '%r' % dt0.fromordinal(737397) == \
           'datetime.datetime(2019, 12, 4, 0, 0)'

    assert '%r' % DateTime.fromtimestamp(1575417598.0, JST) == \
           '%r' % dt1.fromtimestamp(1575417598.0, JST) == \
           "DateTime('2019-12-04 08:59:58+09:00')"
    assert '%r' % datetime.fromtimestamp(1575417598.0, JST) == \
           '%r' % dt0.fromtimestamp(1575417598.0, JST) == (
           'datetime.datetime(2019, 12, 4, 8, 59, 58, tzinfo=%r)' % JST)

    assert dt1.hour == dt0.hour == 8

    assert dt1.isocalendar() == dt0.isocalendar() == (2019, 49, 3)

    assert dt1.isoformat() == dt0.isoformat() == '2019-12-04T08:59:58'

    assert dt1.isoweekday() == dt0.isoweekday() == 3

    assert '%r' % [dt1.min, dt1.max] == (
        "[DateTime('0001-01-01 00:00:00+00:00'), "
        "DateTime('9999-12-31 23:59:59+00:00')]")
    assert '%r' % [dt0.min, dt0.max] == (
        '[datetime.datetime(1, 1, 1, 0, 0), '
        'datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)]')
    assert dt1.min != dt0.min
    assert dt1.max != dt0.max

    assert dt1.microsecond == dt0.microsecond == 0

    assert dt1.minute == dt0.minute == 59

    assert dt1.month == dt0.month == 12

    assert ('%r' % DateTime.now()).startswith('DateTime(')
    assert ('%r' % datetime.now()).startswith('datetime.datetime(')

    assert ('%r' % DateTime.today()).startswith('DateTime(')
    assert ('%r' % datetime.today()).startswith('datetime.datetime(')

    assert '%r' % dt1.replace(day=5) == \
        "DateTime('2019-12-05 08:59:58')"
    assert '%r' % dt0.replace(day=5) == \
        'datetime.datetime(2019, 12, 5, 8, 59, 58)'

    assert dt1.resolution == timedelta(seconds=1)
    assert dt0.resolution == timedelta(microseconds=1)

    assert dt1.second == dt0.second == 58

    assert dt1.strftime('%F %T%z') == dt0.strftime('%F %T%z') == \
        '2019-12-04 08:59:58'

    args = ('2019-12-04 08:59:58+0900', '%Y-%m-%d %H:%M:%S%z')
    assert '%r' % DateTime.strptime(*args) == \
        "DateTime('2019-12-04 08:59:58+09:00')"
    assert '%r' % datetime.strptime(*args) == (
        'datetime.datetime(2019, 12, 4, 8, 59, 58, tzinfo=%r)' % JST)

    assert '%r' % dt1.time() == '%r' % dt0.time() == 'datetime.time(8, 59, 58)'

    assert dt1.timestamp() == dt0.timestamp()

    assert dt1.timetuple() == dt0.timetuple()

    assert dt1.timetz() == dt0.timetz()
    assert dt1.astimezone(JST).timetz() == dt0.astimezone(JST).timetz()

    for m in (DateTime.today, dt1.today, datetime.today, dt0.today):
        with pytest.raises(TypeError):
            m(JST)

    assert dt1.toordinal() == dt0.toordinal() == 737397

    assert dt1.tzinfo is dt0.tzinfo is None
    assert dt1.astimezone(JST).tzinfo is dt0.astimezone(JST).tzinfo is JST

    assert dt1.tzname() is dt0.tzname() is None
    assert dt1.astimezone(JST).tzname() == \
        dt0.astimezone(JST).tzname() == 'UTC+09:00'

    assert '%r' % DateTime.utcfromtimestamp(1575417598) == \
        "DateTime('2019-12-03 23:59:58')"
    assert '%r' % datetime.utcfromtimestamp(1575417598) == \
        'datetime.datetime(2019, 12, 3, 23, 59, 58)'

    assert ('%r' % DateTime.utcnow()).startswith('DateTime(')
    assert ('%r' % datetime.utcnow()).startswith('datetime.datetime(')

    assert dt1.utcoffset() is dt0.utcoffset() is None
    assert dt1.astimezone(JST).utcoffset() == \
        dt0.astimezone(JST).utcoffset() == timedelta(hours=9)

    assert dt1.utctimetuple() == dt0.utctimetuple()

    assert dt1.weekday() == dt0.weekday() == 2

    assert dt1.year == dt0.year == 2019


def test_Util_TERM_W():
    assert Util.TERM_W > 0


def test_Util_gz6():
    bin = (
        b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xffc`'
        b'\xa0=\x00\x00\xca\xc6\x88\x99d\x00\x00\x00')
    assert Util.gz6(b"\0" * 100) == bin
    assert gzip.decompress(bin) == b"\0" * 100


def test_Util_init_logger():
    dir = 'py3x.test'
    os.mkdir(dir)

    class Req:
        def dump(self):
            return "[REQ]\n"

    tdy = Util.today
    yst = tdy() - 1
    Util.today = lambda: yst
    Util.init_logger(dir + '/utils.log', more=Req().dump)
    Util.today = tdy
    warn("a\nb?")
    warnings.warn("A\nB?")
    try:
        die('!', 1)
    except Exception as e:
        warn(e, level=logging.ERROR)
    logs = [open(dir + '/' + f).read() for f in sorted(os.listdir(dir))]

    logging.captureWarnings(False)
    hs = logging.getLogger().handlers
    assert ''.join(hs[-1].readlines()) == logs[1]
    hs.clear()
    Util.rm_rf(dir)

    assert len(logs) == 2
    assert logs[0] == ''
    w1, w2, e, x = logs[1].split("\n\n")

    lns = w1.split("\n")
    ln0 = lns[0].split("\t")
    assert ln0[1] == 'WARNING'
    assert ln0[2] == 'test_utils'
    assert ln0[3] == 'a'
    assert lns[1] == "\tb?"
    assert lns[-1] == "\t"
    assert lns[-2] == "\t[REQ]"
    assert lns[-3] == '\t  warn("a\\nb?")'
    assert lns[-4].endswith(', in test_Util_init_logger')

    lns = w2.split("\n")
    ln0 = lns[0].split("\t")
    assert ln0[1] == 'WARNING'
    assert ln0[2] == 'py.warnings'
    assert ln0[3].endswith(': UserWarning: A')
    assert lns[1] == "\tB?"
    assert lns[2] == '\t  warnings.warn("A\\nB?")'

    lns = e.split("\n")
    ln0 = lns[0].split("\t")
    assert ln0[1] == 'ERROR'
    assert ln0[2] == 'test_utils'
    assert ln0[3] == "RuntimeError: ('!', 1)"
    assert lns[1].endswith(', in test_Util_init_logger')
    assert lns[-1] == "\t"
    assert lns[-2] == "\t[REQ]"
    assert lns[-3] == "\t  raise e"


def test_Util_is_bool_str():
    assert Util.is_bool_str('1')
    assert Util.is_bool_str('0')
    assert not Util.is_bool_str(1)
    assert not Util.is_bool_str(True)
    assert not Util.is_bool_str('True')


def test_Util_is_emal_str():
    assert Util.is_email_str('foo@bar.baz')
    assert Util.is_email_str('FOO.@BAR.BAZ')
    assert not Util.is_email_str(None)
    assert not Util.is_email_str('')
    assert not Util.is_email_str(1)
    assert not Util.is_email_str('foo@.bar.baz')


def test_Util_now_epoch():
    assert Util.now_epoch() == int(time.time())


def test_Util_sendmail_():
    sender, rcpt, msg = Util.sendmail_(
        {
            'From': 'ＦＯＯ <foo@bar.baz>',
            'To': 'foo1@bar.baz, foo2@bar.baz',
            'Cc': 'foo3@bar.baz, foo2@bar.baz',
            'Bcc': 'foo4@bar.baz, foo2@bar.baz',
            'Reply-To': 'ＢＡＲ <bar@bar.baz>',
            'Subject': 'ＨＥＬＬＯ',
        },
        "AAA\nＢＢＢ\n",
        ()
    )
    assert sender == 'foo@bar.baz'
    assert rcpt == {
        'foo1@bar.baz', 'foo2@bar.baz', 'foo3@bar.baz', 'foo4@bar.baz'}
    msg = str(msg)
    assert "\nFrom: =?utf-8?b?77ym77yv77yv?= <foo@bar.baz>\n" in msg
    assert "\nTo: foo1@bar.baz, foo2@bar.baz\n" in msg
    assert "\nCc: foo3@bar.baz, foo2@bar.baz\n" in msg
    assert "\nBcc:" not in msg
    assert "\nReply-To: =?utf-8?b?77yi77yh77yy?= <bar@bar.baz>\n" in msg
    assert "\nSubject: =?utf-8?b?77yo77yl77ys77ys77yv?=\n" in msg

    sender, rcpt, msg = Util.sendmail_(
        {
            'From': 'foo@bar.baz',
            'To': 'foo1@bar.baz',
            'Reply-To': '<bar@bar.baz>',
            'Subject': 'TEST',
        },
        "A\n",
        (('foo.txt', 'foo'),)
    )
    assert sender == 'foo@bar.baz'
    assert rcpt == {'foo1@bar.baz'}
    msg = str(msg)
    assert "\nFrom: foo@bar.baz\n" in msg
    assert "\nTo: foo1@bar.baz\n" in msg
    assert "\nReply-To: bar@bar.baz\n" in msg
    assert "\nSubject: TEST\n" in msg
    assert '''
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="foo.txt"

Zm9v

--==============''' in msg


def test_Util_str2val():
    s2v = Util.str2val
    assert s2v(False, bool) is False
    assert s2v(True, bool) is True
    assert s2v(0, bool) is False
    assert s2v(1, bool) is True
    assert s2v(2, bool) == 2
    assert s2v('', bool) is None
    assert s2v('0', bool) is False
    assert s2v('1', bool) is True
    assert s2v('2', bool) == '2'

    assert s2v(10, int) == 10
    assert s2v(0.1, int) == 0.1
    assert s2v('', int) is None
    assert s2v('10', int) == 10
    assert s2v('-1', int) == -1
    assert s2v('0.1', int) == '0.1'
    assert s2v('a', int) == 'a'
    assert s2v('1,000', int) == '1,000'
    assert s2v('1,000', int, False) == 1000

    d = Date('2019-01-31')
    assert s2v('', Date) is None
    assert s2v(d, Date) == d
    assert s2v('2019-01-31', Date) == d
    assert s2v('2019-01-32', Date) == '2019-01-32'

    dt = DateTime('2019-01-31 01:23:00')
    assert s2v('', DateTime) is None
    assert s2v(dt, DateTime) == dt
    assert s2v('2019-01-31 01:23:00', DateTime) == dt
    assert s2v('2019-01-31 01:23', DateTime) == '2019-01-31 01:23'
    assert s2v('2019-01-31 01:23', DateTime, None) == dt
    assert s2v('2019-01-31 01:23:60', DateTime) == '2019-01-31 01:23:60'

    with pytest.raises(KeyError):
        s2v(1, list)


def test_Util_url_encode():
    assert Util.url_encode('\xE9\x87\x8D\xE3\x82\x81=*-.0-9@A-Z_a-zあ') == \
        '%E9%87%8D%E3%82%81%3D*-.0-9@A-Z_a-z%E3%81%82'


def test_XEnum():
    Int = XEnum(
        qw('value label x10'),
        I1=(1, 'one', 10),
        I2=(2, 'two', 20),
    )
    Int.__name__ = 'foo.bar'

    Str = XEnum(
        qw('value label uc'),
        S1=('1', 'a', 'A'),
        S2=('2', 'b', 'B'),
    )

    assert Int.I1 == 1
    assert Int.I1.value == 1
    assert Int.I1.label == 'one'
    assert '%r' % Int.I1 == 'foo.bar.I1'
    assert '%r' % Int == 'foo.bar'
    assert dict(Int.items()) == {1: 'one', 2: 'two'}
    assert Int[1].label == 'one'
    assert Int['1'].label == 'one'
    assert Int[Int.I1].label == 'one'
    assert Int.get(1).label == 'one'
    assert Int.get(0) is None
    assert 1 not in Int
    assert '1' not in Int
    assert Int.I1 in Int
    assert [*Int] == [Int.I1, Int.I2]

    assert Str.S1 == '1'
    assert Str['1'].label == 'a'
    assert Str['2'].uc == 'B'
    assert 1 not in Str
    assert '1' not in Str
    assert Str.S1 in Str
    assert '%r' % Str.S1 == 'S1'
    assert '%r' % Str == 'XEnum'
    assert dict(Str.items()) == {'1': 'a', '2': 'b'}

    with pytest.raises(AttributeError) as e:
        Int.xxx
    assert e.value.args == ("'XEnum' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        Int.xxx = 100
    assert e.value.args == ("'XEnum' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        Int.I1.xxx
    assert e.value.args == ("'foo.bar' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        Int.I1.xxx = 100
    assert e.value.args == ("'foo.bar' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        Int.I1.value = 100
    assert e.value.args == ("'foo.bar' object attribute 'value' is read-only",)

    with pytest.raises(AttributeError) as e:
        Str.S1.xxx
    assert e.value.args == ("'XEnum' object has no attribute 'xxx'",)

    with pytest.raises(AttributeError) as e:
        Str.S1.xxx = 100
    assert e.value.args == ("'XEnum' object has no attribute 'xxx'",)
