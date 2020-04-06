from py3x.orm import BulkLoader, SQL, BETWEEN, IN, LIKE, NOT, database


def test_BulkLoader():
    args = []
    db = database.Database()
    db.execute = lambda s, vs: args.append((s, vs.copy()))
    ldr = BulkLoader(
        db, 'foos', ('id', 'foo'), per=2,
        suffix='ON DUPLICATE KEY UPDATE foo = VALUES(foo)')

    sql = (
        "INSERT INTO foos (id, foo) VALUES\n"
        "(%s, %s),\n"
        "(%s, %s)\n"
        "ON DUPLICATE KEY UPDATE foo = VALUES(foo)")
    ldr.add(1, 'A')
    assert not args
    ldr.add(2, 'B')
    assert args == [(sql, [1, 'A', 2, 'B'])]
    args.clear()
    ldr.extend(((3, 'a'), (4, 'b')))
    assert args == [(sql, [3, 'a', 4, 'b'])]

    ldr.per = 3
    ldr.extend(((3, 'a'), (4, 'b')))
    assert ldr.sql() == SQL(sql, 3, 'a', 4, 'b')

    ldr = BulkLoader(db, 'foos', dict(id='%s', created_at='NOW()'))
    assert ldr._cs == ('(%s, NOW())',)
    assert ldr._nc == 1
    ldr.add(1)
    assert ldr.sql() == SQL(
        "INSERT INTO foos (id, created_at) VALUES\n"
        "(%s, NOW())", 1)


def test_SQL():
    x = SQL('foo = %s OR bar = %s', 'FOO', 'BAR')
    assert(str(x) == 'foo = %s OR bar = %s')
    assert('%r' % x == "SQL('foo = %s OR bar = %s', 'FOO', 'BAR')")
    assert x
    assert not SQL('')

    assert [*x.as_('baz')] == ['(foo = %s OR bar = %s) AS baz', ('FOO', 'BAR')]


def test_Operator():
    assert BETWEEN(100, 200) == SQL('BETWEEN %s AND %s', 100, 200)
    assert IN(0, 1) == IN(i for i in range(2)) == SQL('IN (%s, %s)', 0, 1)
    assert IN() == SQL('IN (NULL)')
    assert IN(SQL('SELECT id FROM foo WHERE x = %s', 100)) == SQL(
        'IN (SELECT id FROM foo WHERE x = %s)', 100)
    assert LIKE('%foo%') == SQL('LIKE %s', '%foo%')
    assert NOT(None) == SQL('IS NOT NULL')
    assert NOT(LIKE('%foo%')) == SQL('NOT LIKE %s', '%foo%')
    assert NOT(1) == SQL('!= %s', 1)
