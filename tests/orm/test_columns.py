from py3x import errors
from py3x.orm import ColumnNotLoaded, database, model
from py3x.orm.columns import _SERIAL, BELONGS_TO, BLOB, BOOL, DATE, DATETIME, \
    INT, TEXT, VARCHAR
from py3x.orm.model import NOW, TODAY
from py3x.utils import Date, DateTime, SRE_Pattern, XEnum, include, qw
from tests.tlib import instantiate
import pytest


class SERIAL(INT):
    include(_SERIAL)


def test_Column():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'
        id = INT(auto_increment=True, primary_key=True)
        name = VARCHAR()

    # __get__
    assert '%r' % Foo.id == 'Foo.id'
    assert '%r' % Foo.name == 'Foo.name'

    foo = Foo()
    assert foo.id is None
    assert 'id' not in foo.__dict__
    assert foo.name is None

    foo.id = 101
    assert foo.id == 101
    assert foo.__dict__['id'] == 101

    foo = instantiate(Foo, id=100, name='foo')
    assert set(foo.__dict__) == {'.k2i', '.dbvs'}
    assert foo.id == 100
    assert foo.name == 'foo'
    assert set(foo.__dict__) == {'.k2i', '.dbvs'}

    foo = instantiate(Foo)
    with pytest.raises(ColumnNotLoaded):
        foo.id

    cnt = [0]

    def db2py(v):
        cnt[0] += 1
        return v + 1

    Foo.id.db2py = db2py

    foo = instantiate(Foo, id=100)
    assert cnt == [0]
    assert foo.id == 101
    assert cnt == [1]
    assert 'id' in foo.__dict__
    assert foo.id == 101
    assert cnt == [1]

    foo = instantiate(Foo, id=100)
    foo.id = 10
    assert 'id' in foo.__dict__
    assert foo.id == 10
    assert cnt == [1]

    Foo.id.db2py = None

    # __init__
    with pytest.raises(TypeError):
        INT(foo=1)

    # _in_
    foo = Foo()
    assert 'id' not in foo
    foo.id = 100
    assert 'id' in foo
    foo = instantiate(Foo, id=100)
    assert 'id' in foo

    # is_changed
    foo = Foo()
    assert not foo.is_changed()
    assert not foo.is_changed('id')
    assert foo.__dict__ == {}

    foo.id = 10
    foo.name = 'f'
    assert foo.is_changed()
    assert foo.is_changed(txn={}) == dict(id=10, name='f')

    foo = instantiate(Foo, id=10)
    assert not foo.is_changed()
    foo.id = 10
    assert not foo.is_changed()
    foo.id = 11
    assert foo.is_changed()
    foo.name = 'f'
    assert foo.is_changed(txn={}) == dict(id=11, name='f')

    Foo.id.py2db = lambda x: x + 1
    assert foo.is_changed('id', txn={}) == {'id': 12}
    assert foo.id == 11
    Foo.id.py2db = None

    class Bar(Base):
        DB_TABLE = 'bars'
        foo_id = BELONGS_TO(Foo)

    foo = instantiate(Foo, id=100, name='foo')
    assert set(foo.__dict__) == {'.k2i', '.dbvs'}
    assert foo.id == 100
    assert foo.name == 'foo'
    assert set(foo.__dict__) == {'.k2i', '.dbvs', 'id'}

    foo = Foo()
    bar = Bar(foo=foo)
    assert bar.is_changed(txn={}) == dict(foo_id=None)
    foo.id = 1
    assert bar.is_changed(txn={}) == dict(foo_id=1)

    # to_be_valid
    assert not Foo().is_valid()
    assert Foo(name='?').is_valid()

    Foo.id.auto_increment = False
    assert not Foo(name='?').is_valid()
    assert Foo(id=1, name='?').is_valid()

    assert not Foo(id=1).is_valid()
    Foo.name.validate['nn'] = False
    assert Foo(id=1).is_valid()

    foo = Foo.instantiate({}, (), None, None)
    assert foo.is_valid()
    foo.id = None
    assert not foo.is_valid()

    Foo.id.auto_increment = True
    Foo.name.validate['nn'] = True

    assert Foo.ddl_sqls() == [
        "CREATE TABLE foos (\n"
        "  id INT NOT NULL PRIMARY KEY,\n"
        "  name VARCHAR(255) NOT NULL\n)"]


def test_types():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'
        c1 = INT()
        c2 = BOOL()
        c3 = DATE()
        c4 = DATETIME()
        c5 = VARCHAR()
        c6 = VARCHAR(null=True, blank=False)
        c7 = TEXT(null=True, validate=dict(re=r'\Ax'))
        c8 = BLOB(null=True)

    assert type(Foo.c7.validate['re']) is SRE_Pattern

    foo = Foo()
    assert not foo.is_valid()
    assert foo.errors == {
        'c1': [errors.BLANK],
        'c2': [errors.BLANK],
        'c3': [errors.BLANK],
        'c4': [errors.BLANK],
        'c5': [errors.BLANK],
    }

    foo['c1'] = 'a'
    foo['c2'] = 'a'
    foo['c3'] = 'a'
    foo['c4'] = 'a'
    foo['c5'] = "\x00"
    foo['c7'] = "x\x00"
    foo['c8'] = "\x00"
    assert foo.c8 == b"\0"
    assert not foo.is_valid()
    assert foo.errors == {
        'c1': [errors.BAD_FORMAT],
        'c2': [errors.BAD_FORMAT],
        'c3': [errors.BAD_FORMAT],
        'c4': [errors.BAD_FORMAT],
        'c5': [errors.BAD_CHARS("'\\x00'")],
        'c7': [errors.BAD_CHARS("'\\x00'")],
    }

    foo['c1'] = '2147483648'
    foo['c2'] = '2'
    foo['c3'] = '2000-00-00'
    foo['c4'] = '2000-00-00 00:00:00'
    foo['c5'] = "\n"
    foo['c6'] = "\r"
    foo['c7'] = "x\r"
    foo['c8'] = "\n"
    assert foo.c8 == b"\n"
    assert not foo.is_valid()
    assert foo.errors == {
        'c1': [errors.TOO_GREAT(2147483647)],
        'c2': [errors.BAD_FORMAT],
        'c3': [errors.BAD_FORMAT],
        'c4': [errors.BAD_FORMAT],
        'c5': [errors.BAD_CHARS('[LF]')],
        'c6': [errors.BAD_CHARS('[CR]')],
        'c7': [errors.BAD_CHARS('[CR]')],
    }

    foo['c1'] = '-2147483648'
    foo['c2'] = '0'
    foo['c3'] = '2000-01-01'
    foo['c4'] = '2000-01-01 00:00:00'
    foo['c5'] = " "
    foo['c6'] = ''
    foo['c7'] = "x\n"
    assert foo.is_valid()
    assert foo.errors == {}

    foo['c1'] = ''
    assert foo.c1 is None
    foo['c1'] = '-12'
    assert foo.c1 == -12
    foo['c1'] = 'a'
    assert foo.c1 == 'a'
    foo['c1'] = 123
    assert foo.c1 == 123

    foo['c2'] = ''
    assert foo.c2 is None
    foo['c2'] = '1'
    assert foo.c2 is True
    foo['c2'] = '0'
    assert foo.c2 is False
    foo['c2'] = 'T'
    assert foo.c2 == 'T'
    foo['c2'] = True
    assert foo.c2 is True

    foo['c3'] = ''
    assert foo.c3 is None
    foo['c3'] = '2000-01-01'
    assert foo.c3 == Date('2000-01-01')
    foo['c3'] = '2000-02-30'
    assert foo.c3 == '2000-02-30'

    foo['c4'] = ''
    assert foo.c4 is None
    dt = DateTime('2000-01-01 00:00:00')
    foo['c4'] = '2000-01-01'
    assert foo.c4 == dt
    foo['c4'] = '2000-01-01 00'
    assert foo.c4 == dt
    foo['c4'] = '2000-01-01 00:00'
    assert foo.c4 == dt
    foo['c4'] = '2000-01-01 00:00:00'
    assert foo.c4 == dt
    foo['c4'] = '2000-02-30 00:00:00'
    assert foo.c4 == '2000-02-30 00:00:00'

    foo['c5'] = foo['c6'] = ''
    assert foo.c5 == ''
    assert foo.c6 is None

    assert type(NOW(foo)) is DateTime
    assert type(TODAY(foo)) is Date


def test__SERIAL():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'
        id = SERIAL()

    class Bar(Base):
        DB_TABLE = 'bars'
        id = SERIAL()
        foo_id = BELONGS_TO(Foo)

    assert Bar.ddl_sqls() == [
        'CREATE TABLE bars (\n'
        '  id SERIAL PRIMARY KEY,\n'
        '  foo_id INT NOT NULL,\n'
        '  FOREIGN KEY (foo_id) REFERENCES foos (id)\n'
        ')',
        'CREATE INDEX bars_foo_id ON bars (foo_id)']


def test_BELONGS_TO():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'
        id = INT(auto_increment=True, primary_key=True)

    class Bar(Base):
        DB_TABLE = 'bars'
        id = INT(auto_increment=True, primary_key=True)
        foo_id = BELONGS_TO(Foo)

    assert '%r' % Bar.id == 'Bar.id'
    assert '%r' % Bar.foo_id == 'Bar.foo_id'

    bar = Bar()
    assert bar.foo_id is None
    assert bar.__dict__ == {}
    assert 'foo_id' not in bar

    foo = bar.foo = instantiate(Foo, id=10)
    assert bar.__dict__ == {'foo': foo, 'foo_id': 10}
    assert 'foo_id' in bar
    assert bar.foo_id == 10
    assert bar.foo is foo

    foo.id = 11
    assert bar.__dict__ == {'foo': foo, 'foo_id': 10}
    assert bar.foo_id == 10
    assert bar.is_changed(txn={}) == {'foo_id': 10}
    assert bar.__dict__ == {'foo': foo, 'foo_id': 10}

    bar.foo_id = 11
    assert bar.__dict__ == {'foo': foo, 'foo_id': 11}
    assert bar.foo_id == 11
    assert bar.foo is foo

    bar.foo_id = 12
    assert bar.__dict__ == {'foo_id': 12}
    assert 'foo' not in bar.__dict__
    assert bar.foo_id == 12

    bar = Bar()
    assert bar.foo_id is None
    foo = bar.foo = Foo()
    foo.id = 13
    assert bar.foo_id == 13
    assert bar.__dict__ == {'foo': foo}

    bar = Bar()
    assert not bar.is_valid()
    assert 'foo_id' not in bar

    bar = Bar()
    foo = bar.foo = Foo()
    assert bar.__dict__ == {'foo': foo}
    assert 'foo_id' not in bar
    assert bar.is_valid()

    del bar.__dict__['foo']
    assert not bar.is_valid()
    del bar.__dict__['errors']

    bar.foo = None
    assert bar.__dict__ == {'foo': None, 'foo_id': None}
    assert not bar.is_valid()

    bar.foo_id = 'abc'
    assert bar.foo is None
    assert not bar.is_valid()

    assert Bar.foo_id.db_type() == 'INT'

    class DatePk(Base):
        DB_TABLE = 'date_pks'
        id = DATE(primary_key=True)

    class StrPk(Base):
        DB_TABLE = 'str_pks'
        id = VARCHAR(primary_key=True)

    class Foo(Base):
        DB_TABLE = 'foos'
        id = SERIAL()
        date_pk_id = BELONGS_TO(DatePk)
        str_pk_id = BELONGS_TO(StrPk)

    assert Foo.ddl_sqls() == [
        'CREATE TABLE foos (\n'
        '  id SERIAL PRIMARY KEY,\n'
        '  date_pk_id DATE NOT NULL,\n'
        '  str_pk_id VARCHAR(255) NOT NULL,\n'
        '  FOREIGN KEY (date_pk_id) REFERENCES date_pks (id),\n'
        '  FOREIGN KEY (str_pk_id) REFERENCES str_pks (id)\n'
        ')',
        'CREATE INDEX foos_date_pk_id ON foos (date_pk_id)',
        'CREATE INDEX foos_str_pk_id ON foos (str_pk_id)']

    foo = Foo().set_items(date_pk_id='2020-01-27', str_pk_id='aaa')
    assert foo.date_pk_id == Date('2020-01-27')
    assert foo.is_valid()


def test_choices():
    class Base(model.Model):
        DB = database.Database()

    class Foo(Base):
        DB_TABLE = 'foos'
        abc = INT(
            choices=XEnum(
                qw('value label title'),
                A=(1, 'AA', 'aaa'),
                B=(2, 'BB', 'bbb'),
                C=(4, 'CC', 'ccc'),
            ),
            multiple=True, default=3, null=True,
        )
        efg = INT(
            choices={1: 'ONE', 2: 'TWO', 4: 'FOUR'},
            multiple=True, default=2, null=True)
        xyz = VARCHAR(
            choices=XEnum(X='x', Y='y', Z='z'),
            default='x', null=True)
        flag = BOOL(
            choices={True: 'ON', False: 'OFF'},
            default=True, null=True)

    assert '%r' % Foo.ABC.A == 'Foo.ABC.A'

    foo = Foo()
    assert foo.abc_choices() == ['AA', 'BB']
    assert foo.abc_choices('title') == ['aaa', 'bbb']
    assert foo.efg_choices() == ['TWO']
    assert foo.xyz is Foo.XYZ.X
    assert foo.xyz_choice() == 'X'
    assert foo.flag_choice() == 'ON'
    assert foo.is_valid()

    foo['abc'] = ['A', 'AA', '2']
    foo['efg'] = ['4', '3', 'TWO']
    foo['xyz'] = 'Z'
    foo['flag'] = 'f'
    assert foo.abc == ['A', 'AA', Foo.ABC.B]
    assert foo.abc_choices() == ['BB']
    assert foo.abc_choices('title') == ['bbb']
    assert foo.efg == [4, '3', 'TWO']
    assert foo.efg_choices() == ['FOUR']
    assert foo.xyz == 'Z'
    assert foo.xyz_choice() is None
    assert foo.flag == 'f'
    assert foo.flag_choice() is None
    assert not foo.is_valid()
    assert foo.errors == {
        'abc': [errors.INVALID],
        'efg': [errors.INVALID],
        'xyz': [errors.BAD_FORMAT],
        'flag': [errors.BAD_FORMAT],
    }

    foo = instantiate(Foo, abc=None, efg=None, xyz=None, flag=None)
    assert foo.abc is None
    assert foo.abc_choices() == []
    assert foo.efg is None
    assert foo.efg_choices() == []
    assert foo.xyz is None
    assert foo.xyz_choice() is None
    assert foo.flag is None
    assert foo.flag_choice() is None
    foo['flag'] = ''
    assert foo.is_valid()
    assert not foo.is_changed()

    foo = instantiate(Foo, abc=0, efg=0, xyz='x', flag=0)
    assert foo.abc == foo.efg == []
    assert foo.xyz is foo.XYZ.X
    assert foo.flag is False

    foo['flag'] = '1'
    assert foo.flag is True
    assert foo.is_changed()
    foo['flag'] = '0'
    assert not foo.is_changed()

    foo['xyz'] = ''
    assert foo.xyz is None
    assert foo.is_changed()

    foo['xyz'] = 'y'
    assert foo.xyz is Foo.XYZ.Y

    foo = instantiate(Foo, abc=7, efg=7, xyz='z', flag=1)
    assert foo.abc == [Foo.ABC.A, Foo.ABC.B, Foo.ABC.C]
    assert foo.efg == [1, 2, 4]
    assert foo.xyz is foo.XYZ.Z
    assert foo.flag is True

    foo['abc'] = foo['efg'] = ''
    assert foo.abc == ''
    assert foo.efg == ''
    assert not foo.is_valid()
    assert foo.errors == {
        'abc': [errors.BAD_FORMAT],
        'efg': [errors.BAD_FORMAT],

    }

    foo['abc'] = foo['efg'] = []
    assert foo.abc == []
    assert foo.attr_in_db('abc') == [Foo.ABC.A, Foo.ABC.B, Foo.ABC.C]
    assert foo.efg == []
    assert foo.attr_in_db('efg') == [1, 2, 4]
    assert foo.is_valid()
    assert foo.is_changed(txn={}) == {'abc': 0, 'efg': 0}

    foo['abc'] = foo['efg'] = None
    assert foo.abc is foo.efg is None
    assert foo.is_valid()
    assert foo.is_changed(txn={}) == {'abc': None, 'efg': None}

    with pytest.raises(TypeError) as e:
        class E1(Base):
            DB_TABLE = 'xs'
            flag = BOOL(choices={1: 1}, multiple=True)
    assert e.value.args == ('E1.flag: multiple and db2py are incompossible',)

    with pytest.raises(TypeError) as e:
        class E2(Base):
            DB_TABLE = 'xs'
            abc = INT(choices=XEnum(
                A=(1, 'AA'),
                B=(3, 'BB'),
            ), multiple=True)
    assert e.value.args == ('E2.abc: invalid bit E2.ABC.B',)

    with pytest.raises(TypeError) as e:
        class E3(Base):
            DB_TABLE = 'xs'
            abc = INT(choices={1: 'AA', 3: 'BB'}, multiple=True)
    assert e.value.args == ('E3.abc: invalid bit 3',)

    with pytest.raises(TypeError) as e:
        class E4(Base):
            DB_TABLE = 'xs'
            flag = BOOL(choices=XEnum(T=1))
    assert e.value.args == ('E4.flag: XEnum and db2py are incompossible',)

    a = Foo.ABC.A
    assert a == 1
    assert a.name == 'A'
    assert a.value == 1
    assert a.label == 'AA'
    assert a.title == 'aaa'
    assert dict(Foo.ABC.items()) == {1: 'AA', 2: 'BB', 4: 'CC'}
    assert dict(Foo.ABC.items('title')) == {1: 'aaa', 2: 'bbb', 4: 'ccc'}
    assert [*Foo.ABC] == [Foo.ABC.A, Foo.ABC.B, Foo.ABC.C]

    assert Foo.abc.choices is Foo.ABC
    assert Foo.flag.choices == {True: 'ON', False: 'OFF'}

    foo = instantiate(Foo, abc=3, flag=1)
    assert foo.abc == [foo.ABC.A, foo.ABC.B] == [1, 2]
    assert foo.flag is True
    assert foo.flag_choice() == 'ON'

    foo['abc'] = ['2', '4']
    assert foo.abc == [foo.ABC.B, foo.ABC.C] == [2, 4]
    assert foo.attr_in_db('abc') == [foo.ABC.A, foo.ABC.B] == [1, 2]
    assert foo.abc == [foo.ABC.B, foo.ABC.C] == [2, 4]

    foo['flag'] = '0'
    assert foo.flag is False

    assert foo.is_changed(txn={}) == {'abc': 6, 'flag': False}

    foo = Foo()
    assert foo.abc == [foo.ABC.A, foo.ABC.B]
    assert foo.flag is True
    assert foo.is_valid()

    foo['abc'] = ['1', '2']
    foo['flag'] = 1
    assert foo.abc == [foo.ABC.A, foo.ABC.B] == [1, 2]
    assert foo.flag is True
    assert foo.is_valid()

    foo['flag'] = False
    assert foo.flag is False
