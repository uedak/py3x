from py3x.orm import SQL, database, model
from py3x.orm.columns import BELONGS_TO, BOOL, INT, VARCHAR
from py3x.orm.relations import Alias, HasMany, HasOne
from tests.tlib import instantiate, last_x, r2csr
import pytest


def models():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'

        id = INT(primary_key=True)
        foo = VARCHAR()

    class Bar(Base):
        DB_TABLE = 'bars'
        DB_TABLE_AS = 'ba'

        id = INT(primary_key=True)
        foo_id = BELONGS_TO(Foo)
        bar = VARCHAR()

        bazs = HasMany('Baz', order_by='id')
        bar2 = HasOne('Bar2')
        last_baz = HasOne('Baz', lambda q, r: q.where(
            bar_id=r.id, is_last=True))
        x_baz = HasOne('Baz', lambda q, r: q.where(baz='x'))

    class Bar2(Base):
        DB_TABLE = 'bar2s'

        bar_id = BELONGS_TO(Bar, primary_key=True)
        foo_id = BELONGS_TO(Foo)

    class Baz(Base):
        DB_TABLE = 'bazs'
        DB_TABLE_AS = 'bz'

        id = INT(primary_key=True)
        bar_id = BELONGS_TO(Bar)
        baz = VARCHAR()
        is_last = BOOL()

    return Foo, Bar, Bar2, Baz


def test_Alias():
    Foo, Bar, Bar2, Baz = models()
    bar = Alias(Bar, 't1')
    assert bar.id == SQL('t1.id')
    assert bar.bar == SQL('t1.bar')
    with pytest.raises(AttributeError):
        bar.foo


def test_BelongsTo():
    Foo, Bar, Bar2, Baz = models()
    assert Bar.foo.join_key == 'foo'
    assert Bar.foo.reverse_b2 is False
    assert Bar2.foo.reverse_b2 is False
    assert Bar2.bar.reverse_b2 is False
    assert Bar.foo.simple_joins == (('id', 'foo_id'),)

    # __get__
    assert '%r' % Bar.foo == 'Bar.foo'
    bar = Bar()
    assert bar.foo is None
    assert bar.__dict__ == {}

    foo = Foo()
    bar = Bar(foo=foo)
    assert bar.foo is foo

    bar = Bar(foo_id=2)
    assert 'foo' not in bar
    with pytest.raises(NotImplementedError):
        bar.foo
    assert 'foo' not in bar

    DB = Foo.DB
    DB.execute = r2csr(id=2)
    DB.find_cache = {}
    foo = bar.foo
    del DB.execute
    assert foo.id == 2
    assert bar.__dict__['foo'] is foo
    assert 'foo' in bar

    # __set__
    bar.foo_id = None
    assert 'foo' not in bar.__dict__
    assert bar.__dict__['foo_id'] is None
    assert bar.foo is None
    assert 'foo' not in bar

    with pytest.raises(TypeError) as e:
        bar.foo = 2
    assert e.value.args == ('must be Foo, not int',)

    bar.foo_id = 2
    assert 'foo' not in bar
    assert bar.foo is foo
    assert bar.__dict__['foo'] is foo

    # foo = instantiate(Foo, id=2)
    bar = instantiate(Bar, id=3, foo_id=2)
    bar2 = Bar2(foo=foo, bar=bar)
    assert 'foo' not in bar
    assert bar.foo is foo

    bar = instantiate(Bar, id=3, foo_id=2)
    bar2 = Bar2(bar=bar, foo=foo)
    assert 'foo' not in bar
    assert bar.foo is foo

    bar2 = Bar2(foo_id=2, bar_id=3)
    DB.execute = r2csr(id=3)
    assert bar2.bar.id == 3
    del DB.execute

    bar2 = Bar2(foo_id=foo.id)
    bar = bar2.bar = Bar(foo=foo)
    assert 'foo' not in bar2
    assert bar2.foo is foo
    assert 'foo' in bar2
    bar2.foo = Foo()
    assert 'foo_id' not in bar2

    assert Bar.bazs.query().sql() == SQL(
        'SELECT * FROM bazs bz WHERE bz.bar_id = ba.id')

    assert Bar.bazs.as_('t1').sql() == SQL(
        'SELECT * FROM bazs t1 WHERE t1.bar_id = ba.id')

    assert Bar.bazs.query('bz').sql() == SQL(
        'SELECT * FROM bazs WHERE bar_id = bz.id')


def test_HasMany():
    Foo, Bar, Bar2, Baz = models()
    assert Bar.bazs.join_key is None
    assert Bar.bazs.reverse_b2 == 'bar'
    assert Bar.bazs.simple_joins == (('bar_id', 'id'),)

    bar = Bar(id=1)
    bar.bazs = [Baz()]
    baz = bar.bazs[0]
    assert baz.bar_id == 1
    assert baz.bar is bar

    with pytest.raises(TypeError) as e:
        bar.bazs = None
    assert e.value.args == ("'NoneType' object is not iterable",)

    with pytest.raises(TypeError) as e:
        bar.bazs = [1]
    assert e.value.args == ('must be Baz, not int',)


def test_HasOne():
    Foo, Bar, Bar2, Baz = models()
    assert Bar.bar2.join_key == 'bar2'
    assert Bar.bar2.reverse_b2 == 'bar'
    assert Bar.bar2.simple_joins == (('bar_id', 'id'),)
    assert Bar.last_baz.join_key == 'last_baz'
    assert Bar.last_baz.reverse_b2 == 'bar'
    assert Bar.last_baz.simple_joins is False

    DB = Bar.DB

    fc = DB.find_cache = {}
    DB.execute = r2csr(id=2, foo_id=1)
    bar = Bar.find(2)
    assert fc == {('bars', 2): bar}

    DB.execute = r2csr(bar_id=2, foo_id=1)
    bar2 = bar.bar2
    assert last_x() == (
        'SELECT * FROM bar2s t1 WHERE bar_id = %s',
        (2,), tuple)
    assert fc[('bar2s', 2)] is bar2

    DB.execute = r2csr(id=10, bar_id=2, baz=None, is_last=1)
    lbaz = bar.last_baz
    assert last_x() == (
        'SELECT * FROM bazs bz WHERE bar_id = %s AND is_last = %s',
        (2, True), tuple)
    assert fc[('bazs', 10)] is lbaz

    del DB.execute
    assert Bar(id=2).bar2 is bar2
    with pytest.raises(NotImplementedError):
        Bar(id=2).last_baz

    DB.find_cache = None
    baz = bar.last_baz = Baz(bar_id=2)
    assert baz.bar is bar

    baz = bar.x_baz = Baz(bar_id=2)
    with pytest.raises(NotImplementedError):
        baz.bar

    with pytest.raises(TypeError) as e:
        bar.last_baz = 1
    assert e.value.args == ('must be Baz, not int',)
