from py3x.orm import ColumnNotLoaded, SQL, IN, LIKE, NE, database, model
from py3x.orm.columns import BELONGS_TO, BOOL, INT, VARCHAR
from py3x.orm.relations import HasMany, HasOne
from py3x.utils import qw
from tests.tlib import last_x, rs2csr
import pytest
import re


def models():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'

        id = INT(primary_key=True)
        foo = VARCHAR()
        bars = HasMany('Bar', order_by=('id',))

    class Bar(Base):
        DB_TABLE = 'bars'

        id = INT(primary_key=True)
        foo_id = BELONGS_TO(Foo)
        bar = VARCHAR()

        bazs = HasMany('Baz', 'bar_id', order_by='id')
        bar2 = HasOne('Bar2')
        last_baz = HasOne('Baz', lambda q, r: q.where(
            bar_id=r.id, is_last=True))

    class Bar2(Base):
        DB_TABLE = 'bar2s'

        bar_id = BELONGS_TO(Bar, primary_key=True)
        foo_id = BELONGS_TO(Foo)

    class Baz(Base):
        DB_TABLE = 'bazs'

        id = INT(primary_key=True)
        bar_id = BELONGS_TO(Bar)
        baz = VARCHAR()
        is_last = BOOL()

    return Foo, Bar, Bar2, Baz


def test_Paginate():
    Foo, Bar, Bar2, Baz = models()

    q = Foo.query()
    with pytest.raises(TypeError):
        q.current_page
    with pytest.raises(TypeError):
        q.first_of_page
    with pytest.raises(TypeError):
        q.is_first_page
    with pytest.raises(TypeError):
        q.is_last_page
    with pytest.raises(TypeError):
        q.last_of_page
    with pytest.raises(TypeError):
        q.last_page
    with pytest.raises(TypeError):
        q.pages()
    with pytest.raises(TypeError):
        q.per_page

    q = q.page(3, 10)
    q.total_count = 39
    assert q.current_page == 3
    assert q.first_of_page == 21
    assert q.is_first_page is False
    assert q.is_last_page is False
    assert q.last_of_page == 30
    assert q.last_page == 4
    assert [*q.pages()] == [1, 2, 3, 4]
    assert [*q.pages(3)] == [2, 3, 4]
    assert q.per_page == 10


def test_Query():
    Foo, Bar, Bar2, Baz = models()

    q = Baz.query()
    with pytest.raises(TypeError) as e:
        q and True
    assert e.value.args == ('evaluation of Query(no cache) is deprecated',)

    q = Baz.query()
    q._cache = [Baz()]
    assert q
    assert len(q) == 1

    q._cache = []
    assert not q
    assert len(q) == 0

    q._cache = True
    with pytest.raises(NotImplementedError):
        bool(q)

    with pytest.raises(TypeError) as e:
        q.from_('1')
    assert e.value.args == ('must be SQL, not str',)

    with pytest.raises(TypeError) as e:
        q.limit('1')
    assert e.value.args == ('must be int, not str',)

    Foo.DB.execute = rs2csr(('x',), (1,), (2,), (3,))
    assert Foo.where(id=1).pluck('x').fetchone() == 1
    assert [*Foo.where(id=1).pluck('x')] == [1, 2, 3]
    assert last_x() == ('SELECT x FROM foos t1 WHERE id = %s', (1,), tuple)

    q = Baz.query()
    with pytest.raises(TypeError) as e:
        q[0]
    assert re.match(
        r"\A'Query\(no cache, no limit\)' object "
        r"(is not subscriptable|does not support indexing)\Z",
        e.value.args[0])

    rs = q._cache = [Baz(), Baz()]
    assert q[0] is rs[0]

    with pytest.raises(TypeError) as e:
        q._add2([], [], True, None, 1)
    assert e.value.args == (1,)


def test_Query__iter_rows():
    Foo, Bar, Bar2, Baz = models()
    DB = Foo.DB
    DB.execute = rs2csr(
        qw('id foo_id bar'),
        (1, 1, 'A'),
        (2, 1, 'B'),
    )
    foo = Foo.find(1)

    assert foo.bars.sql() == SQL(
        'SELECT * FROM bars t1 WHERE foo_id = %s ORDER BY id', 1)
    assert len(foo.bars) == 2
    assert [*foo.bars][0].foo is not foo

    q = Baz.query()\
        .join('bar')\
        .join('bar2')\
        .join('last_baz')\
        .join('t3', f=Foo, as_='t5', on=SQL('t5.id = t3.foo_id'))\
        .select(t1=('id',), t2='*', t3='*', t4=('id',), t5=('id',))
    assert q.sql() == SQL(
        "SELECT t1.id, t2.*, '|', t3.*, '|', t4.id, t5.id FROM bazs t1 "
        "JOIN bars t2 ON t2.id = t1.bar_id "
        "JOIN bar2s t3 ON t3.bar_id = t2.id "
        "JOIN bazs t4 ON t4.bar_id = t2.id AND t4.is_last = %s "
        "JOIN foos t5 ON t5.id = t3.foo_id", True)
    DB.execute = rs2csr(
        qw('id id foo_id bar | bar_id foo_id | id id'),
        (1, 2, 3, '', '|', 2, 3, '|', 4, 2),
        (2, 3, 4, '', '|', 3, 4, '|', None, None),
        (3, 3, 4, '', '|', 3, 4, '|', None, None),
    )
    with pytest.raises(TypeError) as e:
        len(q)
    assert e.value.args == (
        "object of type 'Query(no cache, no limit)' has no len()",)

    rs = [*q]
    assert len(rs) == 3
    baz = rs[0]
    assert baz.id == 1
    assert baz.bar.id == 2
    assert baz.bar.bar2.foo_id == 3
    assert baz.bar.last_baz.id == 4
    assert baz.bar.bar2.f.id == 2

    baz = rs[1]
    assert baz.id == 2
    assert baz.bar.id == 3
    assert baz.bar.bar2.foo_id == 4
    assert baz.bar.last_baz is None
    assert baz.bar.bar2.f is None

    bar1 = rs[1].bar
    assert rs[2].bar is bar1

    rs = bar1.bazs.join('bar').select(t1='*', t2='*').cache(True)
    assert rs.sql() == SQL(
        "SELECT t1.*, '|', t2.* FROM bazs t1 "
        "JOIN bars t2 ON t2.id = t1.bar_id "
        "WHERE t1.bar_id = %s ORDER BY t1.id", 3)

    DB.execute = rs2csr(
        qw('id bar_id | id'),
        (2, 3, '|', 3),
        (3, 3, '|', 3),
    )
    assert [*rs][0].bar is not bar1

    q = Foo.query().join('bars').select(t2=('id',), t1='*')
    assert q.sql() == SQL(
        'SELECT t2.id, t1.* FROM foos t1 '
        'JOIN bars t2 ON t2.foo_id = t1.id')
    DB.execute = rs2csr(
        qw('id id foo'),
        (1, 2, ''),
        (3, None, None),
    )
    rs = [*q]
    assert rs[0].foo.id == 2
    assert rs[1].foo is None
    with pytest.raises(ColumnNotLoaded):
        rs[0].foo_id
    with pytest.raises(ColumnNotLoaded):
        rs[1].foo_id

    q = Bar.query().join('bar2').select(t1=('id',), t2='*')
    assert q.sql() == SQL(
        "SELECT t1.id, t2.* FROM bars t1 "
        "JOIN bar2s t2 ON t2.bar_id = t1.id")
    DB.execute = rs2csr(qw('id bar_id foo_id'), (1, 1, 2))
    rs = [*q]
    del DB.execute
    assert rs[0].bar2.bar is rs[0]

    q = Bar.query().join('last_baz').select(t1=('id',), t2='*')
    assert q.sql() == SQL(
        "SELECT t1.id, t2.* FROM bars t1 "
        "JOIN bazs t2 ON t2.bar_id = t1.id AND t2.is_last = %s", True)
    DB.execute = rs2csr(qw('id id bar_id baz is_last'), (1, 3, 1, '', True))
    rs = [*q]
    del DB.execute
    assert rs[0].last_baz.bar is rs[0]


def test_Query_cache():
    Foo, Bar, Bar2, Baz = models()

    def q(x):
        q = Foo.query()
        q._cache = x
        return q

    assert q(None).cache(None)._cache is True
    assert q(None).cache(True)._cache is True
    assert q(None).cache(False)._cache is False

    assert q(True).cache(None)._cache is True
    assert q(True).cache(True)._cache is True
    assert q(True).cache(False)._cache is False

    assert q(False).cache(None)._cache is False
    assert q(False).cache(True)._cache is True
    assert q(False).cache(False)._cache is False

    assert q([]).cache(None)._cache is True
    assert q([]).cache(True)._cache == []
    assert q([]).cache(False)._cache is False

    assert q([1]).cache() == [1]
    with pytest.raises(TypeError) as e:
        q(None).cache('a')
    assert e.value.args == ('must be bool or None, not str',)


def test_Query_delete():
    Foo, Bar, Bar2, Baz = models()

    assert Baz.as_('').delete().sql() == SQL('DELETE FROM bazs')

    q = Baz.where(id=1).delete()
    assert q.sql() == SQL('DELETE t1 FROM bazs t1 WHERE id = %s', 1)

    args = []
    Foo.DB.execute = lambda *a: args.append(a) or 10
    assert q.join('bar').execute() == 10
    assert args == [(
        'DELETE t1 FROM bazs t1 JOIN bars t2 ON t2.id = t1.bar_id '
        'WHERE t1.id = %s', (1,), int)]


def test_Query_update():
    Foo, Bar, Bar2, Baz = models()

    q = Baz.where(id=1).update(bar_id=100, baz=SQL('baz + 1'))
    assert q.sql() == SQL(
        'UPDATE bazs t1 SET bar_id = %s, baz = baz + 1 WHERE id = %s', 100, 1)
    assert q.join('bar').sql() == SQL(
        'UPDATE bazs t1 JOIN bars t2 ON t2.id = t1.bar_id '
        'SET t1.bar_id = %s, t1.baz = baz + 1 WHERE t1.id = %s', 100, 1)
    assert q.join('bar').update('t2', id='?').sql() == SQL(
        'UPDATE bazs t1 JOIN bars t2 ON t2.id = t1.bar_id '
        'SET t2.id = %s WHERE t1.id = %s', '?', 1)

    with pytest.raises(TypeError) as e:
        q.update()
    assert e.value.args == (
        'update() takes some keyword arguments but 0 were given',)

    with pytest.raises(TypeError) as e:
        q.update(x=1)
    assert e.value.args == ("unknown column: 'x'",)


def test_Query_join():
    Foo, Bar, Bar2, Baz = models()

    q = Foo.query().join(Bar, as_='t2', on=SQL('t2.foo_id = t1.id'))
    assert q.sql() == SQL(
        "SELECT t1.* FROM foos t1 JOIN bars t2 ON t2.foo_id = t1.id")

    q = Foo.query().join('t1', bar=Bar, as_='t2', on=SQL('t2.foo_id = t1.id'))\
        .select(t1='*', t2='*')
    assert q.sql() == SQL(
        "SELECT t1.*, '|', t2.* FROM foos t1 "
        "JOIN bars t2 ON t2.foo_id = t1.id")

    q = Foo.query().join(Foo.bars.on(bar='0'), as_='b0').where('b0', id=0)
    assert q.sql() == SQL(
        'SELECT t1.* FROM foos t1 '
        'JOIN bars b0 ON b0.foo_id = t1.id AND b0.bar = %s '
        'WHERE b0.id = %s', '0', 0)

    q = Foo.query().join(bar0=Foo.bars.on(bar='0')).select('*.*')
    assert q.sql() == SQL(
        "SELECT t1.*, '|', t2.* FROM foos t1 "
        "JOIN bars t2 ON t2.foo_id = t1.id AND t2.bar = %s", '0')

    q = Foo.query().join('bars').join('bazs').join('bar2', as_='b2')\
        .join('last_baz', as_='lb')\
        .left_join('b2', foo2=Foo, as_='f2', on=SQL('f2.id = b2.foo_id'))\
        .select(t2=('bar',), b2=('foo_id',), f2=('id',), lb=('id',))
    sql = (
        'SELECT t2.bar, b2.bar_id, b2.foo_id, f2.id, lb.id FROM foos t1 '
        'JOIN bars t2 ON t2.foo_id = t1.id '
        'JOIN bazs t3 ON t3.bar_id = t2.id '
        'JOIN bar2s b2 ON b2.bar_id = t2.id '
        'JOIN bazs lb ON lb.bar_id = t2.id AND lb.is_last = %s '
        'LEFT JOIN foos f2 ON f2.id = b2.foo_id')
    assert q.sql() == SQL(sql, True)
    assert q.last_t() == 'f2'

    with pytest.raises(TypeError) as e:
        q.join('bar2', foo=1, bar=2)
    assert e.value.args == (
        'join() takes 0 or 1 keyword arguments but 2 were given',)

    with pytest.raises(TypeError) as e:
        assert q.join(on=1)
    assert e.value.args == (
        'join(on=...) takes 1 positional argument but 0 were given',)

    with pytest.raises(TypeError) as e:
        assert q.join('x', f3=1, on=1)
    assert e.value.args == ("no table alias for 'x'",)

    with pytest.raises(TypeError) as e:
        assert q.join('t1', f3=1, on=1)
    assert e.value.args == ('must be ModelClass, not int',)

    with pytest.raises(TypeError) as e:
        assert q.join(1, 2, f3=1)
    assert e.value.args == (
        'join(rel=...) takes 0 or 1 positional arguments but 2 were given',)

    with pytest.raises(TypeError) as e:
        assert q.join('t2', f3=1)
    assert e.value.args == ("keyword argument 'f3' must be Relation, not int",)

    with pytest.raises(TypeError) as e:
        assert q.join(1, 2, 3)
    assert e.value.args == (
        'join() takes 1 or 2 positional arguments but 3 were given',)

    with pytest.raises(TypeError) as e:
        assert q.join('t1', SQL('?'))
    assert e.value.args == ("unknown relation: SQL('?')",)

    with pytest.raises(TypeError) as e:
        q.join('foo')
    assert e.value.args == ("relation 'foo' is ambiguous",)

    with pytest.raises(TypeError) as e:
        Bar.as_('').join('foo')
    assert e.value.args == ('no table alias for Bar',)

    with pytest.raises(TypeError) as e:
        assert q.join('last_baz', as_='')
    assert e.value.args == ('no table alias for Baz',)

    with pytest.raises(TypeError) as e:
        q.join('last_baz', as_='t1')
    assert e.value.args == ("not unique table alias: 't1'",)

    with pytest.raises(TypeError) as e:
        q.join('last_baz', as_='lb')
    assert e.value.args == ("not unique table alias: 'lb'",)

    assert q.join('b2', 'foo').sql() == SQL(
        sql + ' JOIN foos t7 ON t7.id = b2.foo_id', True)

    assert Bar.as_('x').join('foo').sql() == SQL(
        'SELECT x.* FROM bars x JOIN foos t1 ON t1.id = x.foo_id')

    with pytest.raises(TypeError) as e:
        assert q.select(lb='*', t1='*')
    assert e.value.args == ("'t1' is unreachable",)

    with pytest.raises(TypeError) as e:
        assert q.select(t1='*', t2='*')
    assert e.value.args == ("'t2' is unreachable",)


def test_Query_order_by():
    Foo, Bar, Bar2, Baz = models()

    q = Bar.query().join('foo').order_by(
        't2.id ASC', 't1.id DESC', SQL('FOO(%s)', 100))
    assert q.sql() == SQL(
        'SELECT t1.* FROM bars t1 JOIN foos t2 ON t2.id = t1.foo_id '
        'ORDER BY t2.id ASC, t1.id DESC, FOO(%s)', 100)

    with pytest.raises(TypeError) as e:
        q.order_by()
    assert e.value.args == (
        'order_by() takes some positional arguments but 0 were given',)

    with pytest.raises(TypeError) as e:
        q.order_by('a b c')
    assert e.value.args == ("unknown column: 'a b c'",)

    with pytest.raises(TypeError) as e:
        q.order_by('xxx')
    assert e.value.args == ("unknown column: 'xxx'",)

    with pytest.raises(TypeError) as e:
        q.order_by('id')
    assert e.value.args == ("column 'id' is ambiguous",)

    with pytest.raises(TypeError) as e:
        q.order_by(1)
    assert e.value.args == ("unknown column: 1",)


def test_Query_select():
    Foo, Bar, Bar2, Baz = models()

    q = Baz.where(id=1)\
        .select('id', SQL('COUNT(1)'))\
        .from_(SQL('bazs'))\
        .group_by(SQL('id'))\
        .having(SQL('COUNT(1) > 1'))\
        .order_by('id DESC', SQL('1'))\
        .limit(1)\
        .offset(10)\
        .for_update()
    assert q.sql() == SQL(
        'SELECT id, COUNT(1) FROM bazs WHERE id = %s '
        'GROUP BY id HAVING COUNT(1) > 1 '
        'ORDER BY id DESC, 1 LIMIT %s OFFSET %s FOR UPDATE', 1, 1, 10)

    args = []
    Foo.DB.execute = lambda *a: args.append(a) or [10]
    assert q.count() == 10
    assert args == [(
        'SELECT COUNT(1) FROM bazs WHERE id = %s '
        'GROUP BY id HAVING COUNT(1) > 1', (1,), 1)]
    assert q.total_count == 10

    args.clear()
    Foo.DB.execute = lambda *a, **kw: args.append(a) or ()
    assert not q.exists()
    assert args == [(
        'SELECT 1 FROM bazs WHERE id = %s '
        'GROUP BY id HAVING COUNT(1) > 1 LIMIT 1', (1,), 1)]

    args.clear()
    Foo.DB.execute = lambda *a, **kw: args.append((a, kw)) or (1,)
    assert q.exists()

    assert q.exists_sql() == SQL(
        'EXISTS (SELECT 1 FROM bazs WHERE id = %s '
        'GROUP BY id HAVING COUNT(1) > 1)', 1)

    q = q.select('id')\
        .from_(None)\
        .group_by(None)\
        .having(None)\
        .order_by(None)\
        .limit(None)\
        .offset(None)\
        .for_update(None)
    assert q.sql() == SQL('SELECT id FROM bazs t1 WHERE id = %s', 1)

    with pytest.raises(TypeError) as e:
        q.select()
    assert e.value.args == (
        'select() takes some positional/keyword arguments but 0 were given',)

    with pytest.raises(TypeError) as e:
        q.select(t2='*')
    assert e.value.args == ("no table alias for 't2'",)

    with pytest.raises(TypeError) as e:
        q.select(None)
    assert e.value.args == ('unknown column: None',)

    with pytest.raises(TypeError) as e:
        q.select('x')
    assert e.value.args == ("unknown column: 'x'",)

    with pytest.raises(TypeError) as e:
        Foo.query().select(t1=())
    assert e.value.args == ("no columns for 't1'",)

    assert Foo.query().select('id', 'foo').sql() == SQL(
        'SELECT id, foo FROM foos t1')

    assert Bar.query().join('foo').select('*.*').sql() == SQL(
        "SELECT t1.*, '|', t2.* FROM bars t1 "
        "JOIN foos t2 ON t2.id = t1.foo_id")

    assert Bar.query().join('foo').select('id', t1=qw('id bar')).sql() == SQL(
        'SELECT t1.id, t1.bar FROM bars t1 JOIN foos t2 ON t2.id = t1.foo_id')


def test_Query_where():
    Foo, Bar, Bar2, Baz = models()

    q = Bar.where(SQL('1 = 0'), bar=SQL('NOW()'), foo_id=IN('a', 'b'))\
        .or_where(foo_id=None)
    assert q.sql() == SQL(
        'SELECT * FROM bars t1 '
        'WHERE 1 = 0 AND bar = NOW() AND foo_id IN (%s, %s) '
        'OR foo_id IS NULL',
        'a', 'b')

    q = q.join('foo')
    assert q.sql() == SQL(
        'SELECT t1.* FROM bars t1 '
        'JOIN foos t2 ON t2.id = t1.foo_id '
        'WHERE 1 = 0 AND t1.bar = NOW() AND t1.foo_id IN (%s, %s) '
        'OR t1.foo_id IS NULL',
        'a', 'b')

    with pytest.raises(TypeError) as e:
        q = q.where('x')
    assert e.value.args == ("no table alias for 'x'",)

    with pytest.raises(TypeError) as e:
        q = q.where('t2', 'id')
    assert e.value.args == ('must be SQL, not str',)

    with pytest.raises(TypeError) as e:
        q = q.where(xxx=1)
    assert e.value.args == ("unknown column: 'xxx'",)

    with pytest.raises(TypeError) as e:
        q = q.where(id=1)
    assert e.value.args == ("column 'id' is ambiguous",)

    q = Baz.where(id=1)\
        .where(Baz.where(baz=LIKE('%_%')).or_where(baz=NE('!')).where_sql())
    assert q.sql() == SQL(
        'SELECT * FROM bazs t1 WHERE id = %s AND (baz LIKE %s OR baz != %s)',
        1, '%_%', '!')

    q = Bar.query()
    assert q.where(Baz.query().where_sql()) is q
    assert q.sql() == SQL('SELECT * FROM bars t1')

    q = Foo.where(SQL.EXISTS(Foo.bars.as_('').where(bar='1')))
    assert q.sql() == SQL(
        'SELECT * FROM foos t1 WHERE EXISTS '
        '(SELECT 1 FROM bars WHERE foo_id = t1.id AND bar = %s)', '1')

    vs = tuple(range(12))
    q = Foo.where()
    for v in vs:
        q = q.or_where(id=v)
    assert q.sql() == SQL(
        'SELECT * FROM foos t1 WHERE ' + ' OR '.join('id = %s' for v in vs),
        *vs)
    w0, w1 = q.kw['where']
    assert len(w0) == 1
    assert len(w0[0]) == 21
    assert w1 == ('OR', ('t1', 'id = %s', 11))


def test_Query_with_select():
    Foo, Bar, Bar2, Baz = models()

    q = Baz.query()
    assert q.sql() == SQL('SELECT * FROM bazs t1')
    assert q.select('*').sql() == SQL('SELECT * FROM bazs t1')
    assert q.select('id').with_select('baz').sql() == \
        SQL('SELECT id, baz FROM bazs t1')

    with pytest.raises(TypeError) as e:
        q.with_select(1)
    assert e.value.args == ('unknown column: 1',)

    q = q.left_join('bar').join('foo').select(t2=('id',))
    assert q.sql() == SQL(
        "SELECT t2.id FROM bazs t1 "
        "LEFT JOIN bars t2 ON t2.id = t1.bar_id "
        "JOIN foos t3 ON t3.id = t2.foo_id")
    assert q.with_select(t3='*').sql() == SQL(
        "SELECT t2.id, t3.* "
        "FROM bazs t1 "
        "LEFT JOIN bars t2 ON t2.id = t1.bar_id "
        "JOIN foos t3 ON t3.id = t2.foo_id")
