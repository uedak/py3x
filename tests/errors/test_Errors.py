from py3x.errors import Errors
from py3x.utils import Date, DateTime, Util
import py3x.errors as errors
import pytest
import re


def test_add():
    es = Errors()
    assert es.BLANK == errors.BLANK

    with pytest.raises(TypeError) as e:
        es.add(None, 1)
    assert str(e.value) == '1'

    with pytest.raises(TypeError) as e:
        es.add(None, errors.TOO_GREAT)
    assert str(e.value) == 'errors.TOO_GREAT'

    assert es.add(None, errors.BLANK) == es
    assert es.add(None, errors.BLANK) == es
    assert es.get(None) == [errors.BLANK]
    assert es.get('foo') == ()

    es = Errors({'foo': [errors.BLANK]})
    assert es.get('foo') == [errors.BLANK]


def test_find():
    es = errors.Errors()
    e1 = errors.BLANK
    e2 = errors.BAD_CHARS('!')
    es.add('k1', e1)
    es.add('k2', e2)

    assert es.find(e1) is e1
    assert es.find(e2) is e2
    assert es.find(errors.BAD_CHARS) is e2

    assert es.find('k1', e1) is e1
    assert es.find('k1', e2) is None
    assert es.find('k1', errors.BAD_CHARS) is None

    assert es.find('k2', e1) is None
    assert es.find('k2', e2) is e2
    assert es.find('k2', errors.BAD_CHARS) is e2

    assert es.find('k3', e1) is None
    assert es.find('k3', e2) is None
    assert es.find('k3', errors.BAD_CHARS) is None

    with pytest.raises(TypeError) as e:
        es.find()
    assert e.value.args == (
        'find() takes 1 or 2 positional arguments but 0 were given',)


def test_validate():
    es = Errors()
    cast = dict(cast=Util.str2val)
    assert not es.clear().validate(None, None, nn=True)
    assert es.get(None) == [errors.BLANK]

    assert not es.clear().validate(None, None, nn=errors.UNSELECTED)
    assert es.get(None) == [errors.UNSELECTED]

    assert es.clear().validate(None, 'a', nn=True)
    assert not es

    assert not es.clear().validate(None, 'a', **cast, type=int)
    assert es.get(None) == [errors.BAD_FORMAT]

    assert es.clear().validate(None, '1', **cast, type=int)
    assert not es

    assert es.clear().validate(None, 100, type=int)
    assert not es

    assert es.clear().validate(None, '', **cast, type=int)
    assert not es

    assert not es.clear().validate(None, 2147483648, type=int)
    assert es.get(None) == [errors.TOO_GREAT(2147483647)]

    for v in (None, True, False):
        assert es.clear().validate(None, v, type=bool)
        assert not es

    for v in ('', 0, '0', 'a'):
        assert not es.clear().validate(None, v, type=bool)
        assert es.get(None) == [errors.BAD_FORMAT]

    for v in ('', 0, '0'):
        assert es.clear().validate(None, v, **cast, type=bool)
        assert not es

    assert not es.clear().validate(None, 'a', **cast, type=bool)
    assert es.get(None) == [errors.BAD_FORMAT]


def test_validate_chars():
    es = Errors()
    r = re.compile(r'[A-Z]+')
    assert es.clear().validate(None, 'ABC', chars=r)
    assert not es

    assert not es.clear().validate(None, 'abc', chars=r)
    assert es.get(None) == [errors.BAD_CHARS("'a', 'b', 'c'")]

    assert not es.clear().validate(None, ' abc', chars=r)
    assert es.get(None) == [errors.BAD_CHARS("[SP], 'a', 'b', ...")]


def test_validate_choice():
    es = Errors()
    assert es.clear().validate(None, 'A', choice={'A'})
    assert not es.clear().validate(None, 'B', choice={'A'})
    assert es.get(None) == [errors.INVALID]


def test_validate_choices():
    es = Errors()
    assert es.clear().validate(None, ['A', 'B'], choices={'A', 'B'})
    assert not es.clear().validate(None, ['C'], choices={'A', 'B'})
    assert es.get(None) == [errors.INVALID]


def test_validate_len():
    es = Errors()
    assert es.clear().validate(None, 'abc', len=(3, 3))
    assert not es

    assert not es.clear().validate(None, 'ab', len=(3, 4))
    assert es.get(None) == [errors.TOO_SHORT(3, 2)]

    assert not es.clear().validate(None, 'abcd', len=(2, 3))
    assert es.get(None) == [errors.TOO_LONG(3, 4)]


def test_validate_nn():
    es = Errors()
    assert es.clear().validate_nn(None, 0, True)
    assert not es

    assert not es.clear().validate_nn(None, '', True)
    assert es.get(None) == [errors.BLANK]

    assert not es.clear().validate_nn(None, '', errors.UNSELECTED)
    assert es.get(None) == [errors.UNSELECTED]


def test_validate_range():
    es = Errors()
    assert es.clear().validate(None, 3, range=(3, 3))
    assert not es

    assert not es.clear().validate(None, 2, range=(3, 4))
    assert es.get(None) == [errors.TOO_LITTLE(3)]

    assert not es.clear().validate(None, 4, range=(2, 3))
    assert es.get(None) == [errors.TOO_GREAT(3)]

    t = DateTime('2018-03-23 11:30:00')
    e = t.epoch()
    assert es.validate(None, t, range=(e, e))
    assert es.validate(None, t, range=(t, t))

    assert not es.clear().validate(None, t - 1, range=(t, None))
    assert es.get(None) == [errors.TOO_EARLY('2018-03-23 11:30')]
    assert not es.clear().validate(None, t, range=(e + 1, None))
    assert es.get(None) == [errors.TOO_EARLY('2018-03-23 11:30:01')]

    assert not es.clear().validate(None, t + 1, range=(None, e))
    assert es.get(None) == [errors.TOO_LATE('2018-03-23 11:30')]
    assert not es.clear().validate(None, t, range=(None, t - 1))
    assert es.get(None) == [errors.TOO_LATE('2018-03-23 11:29:59')]

    d = Date('2018-03-23')
    e = d.epoch()
    assert es.validate(None, d, range=(e, e))
    assert es.validate(None, d, range=(d, d))

    assert not es.clear().validate(None, d, range=(e + 1, None))
    assert es.get(None) == [errors.TOO_EARLY('2018-03-24')]
    assert not es.clear().validate(None, d, range=(d + 1, None))
    assert es.get(None) == [errors.TOO_EARLY('2018-03-24')]

    assert not es.clear().validate(None, d, range=(None, e - 1))
    assert es.get(None) == [errors.TOO_LATE('2018-03-22')]
    assert not es.clear().validate(None, d, range=(None, d - 1))
    assert es.get(None) == [errors.TOO_LATE('2018-03-22')]


def test_validate_re():
    es = Errors()
    r = re.compile(r'\A[A-Z]+\Z')
    assert es.clear().validate(None, 'ABC', re=r)
    assert not es

    assert not es.clear().validate(None, 'abc', re=r)
    assert es.get(None) == [errors.BAD_FORMAT]


def test_validate_type():
    es = Errors()
    assert es.clear().validate_type(None, None, int)
    assert not es

    assert es.clear().validate_type(None, -123, int)
    assert not es

    assert not es.clear().validate_type(None, '-123', int)
    assert es.get(None) == [errors.BAD_FORMAT]
