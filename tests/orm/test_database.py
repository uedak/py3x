from py3x.orm.database import Database
from py3x.utils import die
import pytest


def test_DB_close():
    db = Database()
    assert db.close() is None


def test_DB_debug():
    db = Database()
    sql = (
        'SELECT SUM(1) FROM foos t1 '
        'JOIN bars t2 ON t2.foo_id = t1.id')
    sql = f'{sql} WHERE EXISTS({sql})'
    sql = f'SELECT SUM(%s) FROM ({sql})'
    res = []
    db.txn_depth = 1
    db.quote = lambda v: str(v)
    db.debug(sql, (2,), print=lambda x, end: res.append(x))
    assert res[0] == '''  SELECT SUM(2)
    FROM (
      SELECT SUM(1)
      FROM foos t1
      JOIN bars t2 ON t2.foo_id = t1.id
      WHERE EXISTS(
        SELECT SUM(1) FROM foos t1 JOIN bars t2 ON t2.foo_id = t1.id
      )
    )'''

    with pytest.raises(TypeError) as e:
        db.debug('SELECT %s', (1, 2))
    assert e.value.args == (
        'not all arguments converted during string formatting',
        'SELECT %s', (1, 2))


def test_DB_quote():
    db = Database()
    db.quote = lambda v: v
    assert db.debug_quote('123456', limit=5) == '12...'


def test_Transaction():
    db = Database()
    xs = []

    class Csr:
        def execute(self, sql, vs):
            xs.append(sql)

    class Con:
        def begin(self):
            db.execute('BEGIN')

        def cursor(self):
            return Csr()

    db._con = Con()
    with db.txn_do() as txn1:
        with db.txn_do() as txn2:
            with db.txn_do():
                txn2.rollback()
        txn1.rollback()
    assert xs == [
        'BEGIN',
        'SAVEPOINT p1',
        'SAVEPOINT p2',
        'ROLLBACK TO SAVEPOINT p1',
        'SAVEPOINT p1',
        'RELEASE SAVEPOINT p1',
        'ROLLBACK',
        'BEGIN',
        'COMMIT',
    ]

    xs.clear()
    with pytest.raises(RuntimeError):
        with db.txn_do():
            die('!')
    assert xs == ['BEGIN', 'ROLLBACK']
