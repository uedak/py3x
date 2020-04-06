from py3x.errors import Error, BAD_CHARS, BLANK
import pytest


def test_Error():
    x = BAD_CHARS('x')

    with pytest.raises(TypeError) as e:
        BLANK()
    assert str(e.value) == 'errors.BLANK is not callable'

    with pytest.raises(TypeError) as e:
        x()
    assert str(e.value) == "errors.BAD_CHARS('x') is not callable"

    with pytest.raises(TypeError) as e:
        BAD_CHARS(1, 2, 3)
    assert str(e.value) == \
        'BAD_CHARS() takes 1 positional argument but 3 were given'

    assert BLANK == BLANK
    assert x != BLANK
    assert x != BAD_CHARS
    assert x == BAD_CHARS('x')
    assert x != BAD_CHARS('y')

    assert isinstance(BAD_CHARS, Error)
    assert isinstance(BLANK, Error)
    assert isinstance(x, Error)

    assert BLANK.__repr__() == 'errors.BLANK'
    assert BAD_CHARS.__repr__() == 'errors.BAD_CHARS'
    assert x.__repr__() == "errors.BAD_CHARS('x')"

    assert x.msg == BAD_CHARS.msg % x.args

    assert {BLANK: True}[BLANK] is True

    with pytest.raises(AttributeError) as e:
        BLANK.msg = '?'
    assert e.value.args == ("'Error' object attribute 'msg' is read-only",)

    assert BLANK.as_json() == BLANK.msg
