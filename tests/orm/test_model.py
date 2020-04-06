from py3x.orm import ColumnNotLoaded, IN, RecordNotFound, SQL, database, model
from py3x.orm.columns import BELONGS_TO, BOOL, DATE, DATETIME, INT, VARCHAR
from py3x.orm.model import NOW
from py3x.utils import Date, DateTime, Util, XEnum, qw
from tests.tlib import instantiate, last_x, r2csr, rs2csr
import py3x.errors as errors
import pytest


def models():
    class Base(model.Model):
        DB = database.Database()

    class User(Base):
        DB_TABLE = 'users'
        id = INT(auto_increment=True, primary_key=True)
        name = VARCHAR()

    class Foo(Base):
        DB_TABLE = 'foos'
        id = INT(auto_increment=True, primary_key=True)

    class Bar(Base):
        DB_INDEXES = (
            dict(prefix='UNIQUE', on=('foo_id', 'rad')),
        )
        DB_TABLE = 'bars'
        id = INT(auto_increment=True, primary_key=True)
        foo_id = BELONGS_TO(Foo)
        created_by = BELONGS_TO(User)
        created_at = DATETIME(default=NOW)
        updated_by = BELONGS_TO(User)
        updated_at = DATETIME(default=NOW)
        lock_version = INT(default=1)
        name = VARCHAR(default='?', unique=True, blank=False)
        chks = INT(choices=XEnum(A=1, B=2, C=4), multiple=True)
        rad = BOOL(choices={True: 'ON', False: 'OFF'})

    class Baz(Base):
        DB_TABLE = 'bazs'
        name = VARCHAR()

    return Base, User, Foo, Bar, Baz


def test_Model():
    Base, User, Foo, Bar, Baz = models()

    assert Foo.AUTO_INCREMENT == 'id'

    # COLUMN_DEFAULTS
    bar = Bar()
    assert [c for c in bar.COLUMNS if bar[c] is not None] == \
        ['created_at', 'updated_at', 'lock_version', 'name']

    baz = Baz()
    assert [c for c in baz.COLUMNS if baz[c] is not None] == []

    # LOCK_VERSION
    assert Bar.LOCK_VERSION == 'lock_version'
    assert Baz.LOCK_VERSION is None

    # UPDATED_AT
    assert Bar.UPDATED_AT == 'updated_at'
    assert Baz.UPDATED_AT is None

    # UPDATED_BY
    assert Bar.UPDATED_BY == 'updated_by'
    assert Baz.UPDATED_BY is None

    # ddl_sqls
    assert Bar.ddl_sqls() == [
        'CREATE TABLE bars (\n'
        '  id INT NOT NULL PRIMARY KEY,\n'
        '  foo_id INT NOT NULL,\n'
        '  created_by INT NOT NULL,\n'
        '  created_at DATETIME NOT NULL,\n'
        '  updated_by INT NOT NULL,\n'
        '  updated_at DATETIME NOT NULL,\n'
        '  lock_version INT NOT NULL,\n'
        '  name VARCHAR(255) NOT NULL,\n'
        '  chks INT NOT NULL,\n'
        '  rad BOOL NOT NULL,\n'
        '  FOREIGN KEY (foo_id) REFERENCES foos (id),\n'
        '  FOREIGN KEY (created_by) REFERENCES users (id),\n'
        '  FOREIGN KEY (updated_by) REFERENCES users (id)\n'
        ')',
        'CREATE UNIQUE INDEX bars_foo_id_rad ON bars (foo_id, rad)',
        'CREATE INDEX bars_created_by ON bars (created_by)',
        'CREATE INDEX bars_updated_by ON bars (updated_by)',
        'CREATE UNIQUE INDEX bars_name ON bars (name)']

    assert Baz.ddl_sqls() == [
        'CREATE TABLE bazs (\n'
        '  name VARCHAR(255) NOT NULL\n'
        ')']

    # bulk_loader
    ldr = Foo.bulk_loader()
    assert ldr._sql == ("INSERT INTO foos (id) VALUES\n", '')


def test_find():
    Base, User, Foo, Bar, Baz = models()

    with pytest.raises(TypeError):
        Baz.find(1)

    with pytest.raises(TypeError):
        Bar.find(1, 2)

    with pytest.raises(TypeError):
        Bar.find(SQL('1'))

    with pytest.raises(RecordNotFound):
        Bar.find(None)

    with pytest.raises(RecordNotFound):
        Bar.find('2019-02-31')

    class Baz(Base):
        DB_TABLE = 'bazs'
        id = DATE(primary_key=True)
        name = VARCHAR()

    baz = Baz(id=Date('2019-01-01'), name='?')
    Base.DB.find_cache = {(Baz.DB_TABLE, baz.id): baz}
    assert Baz.find('2019-01-01') is baz

    Base.DB.find_cache = None

    with pytest.raises(TypeError):
        Bar.find(IN(1, 2, 3))

    with pytest.raises(NotImplementedError):
        Baz.find_by(id=IN(1, 2, 3))

    fc = Base.DB.find_cache = {}
    dt1 = Date('2019-01-01')

    Base.DB.execute = r2csr(id=dt1)
    Baz.where().select('id').peek()
    assert not fc

    Base.DB.execute = r2csr(id=dt1, name='A')
    baz1 = Baz.where().select('*').peek()
    assert baz1.name == 'A'
    assert fc == {('bazs', dt1): baz1}

    assert Baz.find_by(id='2019-01-01') is baz1
    assert Baz.find_by(name='A') is not baz1

    dt2 = Date('2019-01-02')
    baz1.id += 1
    args = []
    Base.DB.execute = lambda *a: args.append(a) or 1
    assert baz1.update() == 1
    assert args == [(
        'UPDATE bazs t1 SET id = %s WHERE id = %s',
        (Date('2019-01-02'), Date('2019-01-01')), int)]
    assert fc == {('bazs', dt2): baz1}
    assert Baz.find_by(id='2019-01-02') is baz1

    assert baz1.delete() == 1
    assert fc == {}


def test___init__():
    Base, User, Foo, Bar, Baz = models()
    bar = Bar(name='!', x=1)
    assert bar.name == bar['name'] == '!'
    assert bar.created_at is not None
    assert bar.updated_at is not None
    assert 'name' in bar
    assert 'chks' not in bar

    assert bar['x'] == 1
    assert 'x' in bar
    with pytest.raises(AttributeError):
        bar.x

    bar['x'] = 2
    assert bar['x'] == 2
    with pytest.raises(AttributeError):
        bar.x

    bar['name'] = ''
    assert bar.name is None
    baz = Baz()
    baz['name'] = ''
    assert baz.name == ''

    u2 = User(id=2)
    bar = Bar(by=u2)
    assert bar.created_by == bar.updated_by == 2
    assert bar.creator is bar.updater is u2

    bar = Bar(by=3)
    assert bar.created_by == bar.updated_by == 3


def test_attr_in_db():
    Base, User, Foo, Bar, Baz = models()
    bar = instantiate(Bar, id=1, foo_id=2, chks=3)
    bar['foo'] = Foo(id=3)
    bar['chks'] = ['1', '4']
    assert bar.foo_id == 3
    assert bar.chks == [bar.CHKS.A, bar.CHKS.C]
    assert bar.attr_in_db('foo_id') == 2
    assert bar.attr_in_db('chks') == [bar.CHKS.A, bar.CHKS.B]
    assert bar.chks == [bar.CHKS.A, bar.CHKS.C]


def test_delete():
    Base, User, Foo, Bar, Baz = models()
    bar = instantiate(Bar, id=100)
    assert bar.id == 100
    bar.id = 200
    assert bar.id == 200

    args = []
    Base.DB.execute = lambda *a: args.append(a) or 1
    assert bar.delete() == 1
    assert args == [('DELETE t1 FROM bars t1 WHERE id = %s', (100,), int)]

    with pytest.raises(ColumnNotLoaded) as e:
        Bar().delete()
    assert e.value.args == ('id',)


def test_dict():
    Base, User, Foo, Bar, Baz = models()
    foo = instantiate(Foo, id=1, a=2, b=3)
    foo['b'] = 4
    foo['c'] = '5'
    assert foo.dict() == {'id': 1}
    assert foo.dict(True) == {'id': 1, 'a': 2, 'b': 4, 'c': '5'}


def test_get():
    Base, User, Foo, Bar, Baz = models()

    bar = Bar(id=10, x='X')
    assert bar.get('id') == 10
    assert bar.get('foo') is None
    assert bar.get('rad') is None
    assert bar.get('x') == 'X'
    assert bar.get('y') is None
    assert bar.get('y', 'Y') == 'Y'


def test_getlist():
    Base, User, Foo, Bar, Baz = models()
    bar = Bar(created_by=1, updated_by=1)
    bar['foo_id'] = []
    bar['name'] = None
    bar['chks'] = []
    bar['rad'] = None

    foos = {1: 'foo1', 2: 'foo2'}
    ita = Util.input_tag_attrs
    sto = Util.select_tag_options
    assert [*sto(bar, 'foo_id', foos)] == [
        ('1', False, 'foo1'), ('2', False, 'foo2')]
    assert ita(bar, 'name', {}) == {'name': 'name', 'value': ''}
    assert ita(bar, 'chks', dict(value=2), 'checkbox') == {
        'name': 'chks', 'type': 'checkbox', 'value': '2'}
    assert ita(bar, 'rad', dict(value=False), 'radio') == {
        'name': 'rad', 'type': 'radio', 'value': '0'}

    bar['foo_id'] = '2'
    bar['name'] = '!'
    bar['chks'] = ['1', '2']
    bar['rad'] = '0'
    assert [*sto(bar, 'foo_id', foos)] == [
        ('1', False, 'foo1'), ('2', True, 'foo2')]
    assert ita(bar, 'name', {}) == {
        'name': 'name', 'value': '!'}
    assert ita(bar, 'chks', dict(value=2), 'checkbox') == {
        'name': 'chks', 'type': 'checkbox', 'value': '2', 'checked': '1'}
    assert ita(bar, 'rad', dict(value=False), 'radio') == {
        'name': 'rad', 'type': 'radio', 'value': '0', 'checked': '1'}

    bar = Bar()
    assert bar.getlist('foo_id') == []
    assert bar.getlist('name') == ['?']
    assert bar.getlist('chks') == []
    assert ita(bar, 'chks', dict(value=2, checked=False), 'checkbox') == {
        'name': 'chks', 'type': 'checkbox', 'value': '2'}


def test_restore_attrs():
    Base, User, Foo, Bar, Baz = models()
    _ks = {'name', 'foo', 'foo_id'}

    def _d(x):
        return {k: v for k, v in x.__dict__.items() if k in _ks}

    foo = instantiate(Foo, id=1)
    bar = Bar()
    assert _d(bar) == {'name': '?'}
    bar.name = 'bar'
    bar.foo = foo
    assert _d(bar) == {'foo': foo, 'foo_id': 1, 'name': 'bar'}

    assert bar.name == 'bar'
    bar.restore_attrs()
    assert _d(bar) == {'name': '?'}
    assert bar.name == '?'

    bar = instantiate(Bar, id=2, foo_id=1, name='bar')
    assert bar.foo_id == 1
    assert bar.name == 'bar'
    assert _d(bar) == {'foo_id': 1}

    bar.foo_id = 1
    bar.name = 'bar'
    assert _d(bar) == {'foo_id': 1, 'name': 'bar'}

    bar.restore_attrs()
    assert _d(bar) == {}
    assert bar.foo_id == 1
    assert bar.name == 'bar'
    assert _d(bar) == {'foo_id': 1}

    bar.foo = None
    assert _d(bar) == {'foo': None, 'foo_id': None}
    assert bar.foo_id is None
    bar.restore_attrs()
    assert _d(bar) == {}
    assert bar.foo_id == 1

    bar.foo = foo
    assert _d(bar) == {'foo': foo, 'foo_id': 1}
    assert bar.foo_id == 1
    bar.restore_attrs()
    assert _d(bar) == {'foo': foo}
    assert bar.foo_id == 1

    bar.foo = foo2 = instantiate(Foo, id=2)
    assert _d(bar) == {'foo': foo2, 'foo_id': 2}
    assert bar.foo_id == 2
    bar.restore_attrs()
    assert _d(bar) == {}
    assert bar.foo_id == 1


def test_save():
    Base, User, Foo, Bar, Baz = models()

    args = []
    Base.DB.execute_insert = lambda *a: args.append(a) or 100
    assert Foo().save() == 1
    assert args == [('INSERT INTO foos () VALUES ()', (), 'id')]

    args.clear()
    bar = Bar()
    assert bar.save() == 1
    assert args == [(
        'INSERT INTO bars (created_at, updated_at, lock_version, name) '
        'VALUES (%s, %s, %s, %s)',
        (bar.created_at, bar.updated_at, 1, '?'), 'id')]
    assert bar.__dict__ == {
        '.k2i': bar.SHARED_INDEX,
        '.dbvs': [
            100, None, None, bar.created_at, None, bar.updated_at, 1,
            '?', None, None],
        'created_at': bar.created_at,
        'updated_at': bar.updated_at,
        'name': '?',
    }

    args.clear()
    Base.DB.execute = lambda *a: args.append(a) or 1
    baz = Baz(name='!')
    baz.insert()
    assert args == [('INSERT INTO bazs (name) VALUES (%s)', ('!',))]

    at = DateTime('2019-10-28 18:00:00')
    u10 = User(id=10)
    bar = instantiate(
        Bar, id=1, updated_by=10, updated_at=at, lock_version=1, name='A')
    assert bar.id == 1
    bar.updater = u10
    assert not bar.is_changed()

    args.clear()
    Base.DB.execute = lambda *a: args.append(a) or 1
    assert bar.save(by=User(id=11)) is bar.NO_CHANGES
    assert bar.NO_CHANGES and True
    assert not args
    assert bar.updated_by == 10
    assert bar.__dict__['updater'] is u10
    assert bar.updated_at == at
    assert bar.lock_version == 1
    assert not bar.is_changed()

    bar['id'] = '2'
    bar['name'] = 'B'
    assert bar.is_changed()
    assert bar.save(by=User(id=11), lock=False) == 1
    assert args == [(
        'UPDATE bars t1 SET id = %s, name = %s, updated_at = %s, '
        'updated_by = %s WHERE id = %s',
        (2, 'B', bar.updated_at, 11, 1), int)]
    assert bar.updated_by == 11
    assert 'updater' not in bar.__dict__
    assert bar.updated_at != at
    assert bar.lock_version == 1
    assert not bar.is_changed()

    args.clear()
    bar['lock_version'] = '1'
    assert bar.save(by=12) is bar.NO_CHANGES
    assert not args

    bar['lock_version'] = '2'
    assert bar.save(by=12) == 0
    assert bar.errors == {None: [errors.CONFLICT]}

    assert not args
    bar['name'] = 'C'
    bar['lock_version'] = '1'
    assert bar.save(by=12, timestamp=True) == 1
    assert args == [(
        'UPDATE bars t1 SET name = %s, lock_version = %s, updated_at = %s, '
        'updated_by = %s WHERE id = %s AND lock_version = %s',
        ('C', 2, bar.updated_at, 12, 2, 1), int)]
    assert bar.updated_by == 12
    assert bar.lock_version == 2
    with pytest.raises(TypeError) as e:
        bar.name = '!'
        bar.save(by='?')
    assert e.value.args == (
        "keyword argument 'by' must be int or User, not str",)

    args.clear()
    bar = instantiate(Bar, id=1)
    assert bar.__dict__ == {'.k2i': {'id': 0}, '.dbvs': (1,)}
    bar['name'] = 'D'
    assert bar.save(lock=True) == 1
    assert args == [(
        'UPDATE bars t1 SET name = %s, updated_at = %s, '
        'lock_version = (lock_version + 1) %% 100 WHERE id = %s',
        ('D', bar.updated_at, 1), int)]
    assert bar.updated_at is not None
    assert bar.__dict__ == {
        'name': 'D',
        '.k2i': {'id': 0, 'name': 1, 'updated_at': 2},
        '.dbvs': [1, 'D', bar.updated_at],
    }

    foo = instantiate(Foo, id=1)
    assert foo.save() is foo.NO_CHANGES
    assert foo.save(force=True) is foo.NO_CHANGES


def test_set_items():
    Base, User, Foo, Bar, Baz = models()
    bar = Bar()
    assert bar.set_items(foo_id='1', chks=['1', '4'], bar='BAR')is bar
    assert bar.foo_id == 1
    assert bar.chks == [Bar.CHKS.A, Bar.CHKS.C]
    assert bar['bar'] == 'BAR'


def test_validate():
    Base, User, Foo, Bar, Baz = models()

    bar = Bar()
    bar['lock_version'] = '2'
    bar['chks'] = ['A', 'B']
    assert not bar.is_valid()
    assert bar.errors == {
        'chks': [errors.INVALID],
        'created_by': [errors.BLANK],
        'foo_id': [errors.BLANK],
        'rad': [errors.UNSELECTED],
        'updated_by': [errors.BLANK]}

    bar = instantiate(Bar, id=1, lock_version=1)
    bar['lock_version'] = '2'
    assert not bar.is_valid()
    assert bar.errors == {None: [errors.CONFLICT]}

    bar = instantiate(Bar, id=1, lock_version=2)
    bar['lock_version'] = '2'
    bar['foo_id'] = '10'
    bar['rad'] = '1'
    bar['name'] = '?'

    Base.DB.execute = rs2csr(qw('? ?'), (1, 1))
    assert not bar.is_valid()
    assert bar.errors == {'name': [errors.TAKEN], 'rad': [errors.TAKEN]}
    assert last_x() == (
        'SELECT '
        'EXISTS (SELECT 1 FROM bars t1 WHERE foo_id = %s AND rad = %s),\n'
        'EXISTS (SELECT 1 FROM bars t1 WHERE name = %s)',
        [10, True, '?'], 1
    )

    bar.errors.clear()
    bar['name'] = ''
    bar.validate_column('name', len=(None, 2))
    assert bar.errors == {'name': [errors.BLANK]}

    bar.errors.clear()
    bar.validate_column('name', len=(None, 2), nn=False)
    assert not bar.errors

    bar['name'] = 'abc'
    bar.validate_column('name', len=(None, 2))
    assert bar.errors == {'name': [errors.TOO_LONG(2, 3)]}

    bar['xxx'] = '10'
    bar.errors.clear()
    assert not bar.validate_item('xxx', type=int, range=(1, 5))
    assert bar.errors == {'xxx': [errors.BAD_FORMAT]}

    bar.errors.clear()
    assert not bar.validate_item(
        'xxx', type=int, range=(1, 5), cast=Util.str2int)
    assert bar.errors == {'xxx': [errors.TOO_GREAT(5)]}
