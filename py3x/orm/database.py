from ..utils import cached_property, die
from functools import lru_cache
import os
import re


@lru_cache(256)
def _shared_index(ks):
    return {k: i for i, k in enumerate(ks)}


class Cursor0:
    def __init__(self, csr):
        self.csr = csr

    def __iter__(self):
        return (r[0] for r in self.csr)

    def fetchone(self):
        return self.csr.fetchone()[0]


class Database:
    from .query import Query
    from ..utils import Util
    RE_DEBUG = re.compile(
        r'\(\s*SELECT|[\(\)]|\b(?:'
        r'(?:LEFT |RIGHT |INNER |OUTER |CROSS |FULL |STRAIGHT_)*JOIN|'
        r'FROM|WHERE|(?:ORDER|GROUP) BY|HAVING|UNION)', re.IGNORECASE)
    RE_MIGRATE_SQL = re.compile(r'\A(\d{3})_.*\.sql\Z')

    shared_index = staticmethod(_shared_index)

    def __init__(self, is_debug=False, **kw):
        self.is_debug = is_debug
        self.con_kw = kw
        self.find_cache = None
        self.txn_depth = 0

    @cached_property
    def _con(self):
        return self.connect(**self.con_kw)

    def _con_x(self, x):
        self.txn_depth = 0
        m = getattr(self._con, x, None)
        if m:
            self.is_debug and self.debug(x.upper(), None)
            m()
        else:
            self.execute(x.upper())

    def begin(self):
        self._con_x('begin')
        self.txn_depth = 1
        fc = self.find_cache
        fc and fc.clear()

    def close(self):
        con = self.__dict__.pop('_con', None)
        return con and con.close()

    def commit(self):
        self._con_x('commit')

    def connect(self, **kw):
        raise NotImplementedError

    def debug(self, sql, vs, print=print):
        if "\n" not in sql and (len(sql) >= self.Util.TERM_W or 'JOIN' in sql):
            sql = self.debug_(sql, 0)
        if vs:
            try:
                sql %= tuple(self.debug_quote(v) for v in vs)
            except TypeError as e:
                raise TypeError(*e.args, sql, vs)
        d = self.txn_depth
        if d:
            i = '  ' * d
            sql = i + sql.replace("\n", "\n" + i)
        print(sql, end=";\n")

    def debug_(self, sql, d):
        i = '  ' * d
        if d and len(sql) + (d + 1) * 2 < self.Util.TERM_W:
            return i + sql
        search = self.RE_DEBUG.search
        p0 = p1 = n = 0
        ss = []
        while True:
            m = search(sql, p1)
            if not m:
                ss.append(i + sql[p0:])
                return ''.join(ss)

            g = m.group()
            p = m.start()
            if n:
                n += (1 if g[0] == '(' else -1 if g == ')' else 0)
                if not n:
                    ss.append(self.debug_(sql[p0:p].strip(), d + 1) + "\n  ")
                    p0 = p
            elif g in ('(', ')'):
                pass
            elif g[0] == '(':
                ss.append(i + sql[p0:p] + "(\n  ")
                n = 1
                p0 = p + 1
            else:
                ss.append(i + sql[p0:p - 1] + "\n  ")
                p0 = p
            p1 = m.end()

    def debug_quote(self, v, limit=255):
        v = self.quote(v)
        return v[:limit - 3] + '...' if len(v) > limit - 3 else v

    def execute(self, sql, vs=None, as_=tuple):
        self.is_debug and self.debug(sql, vs)
        csr = self._con.cursor()
        csr.execute(sql, vs)
        return csr if as_ is tuple else \
            csr.rowcount if as_ is int else \
            csr.fetchone() if as_ == 1 else die(as_)

    def execute_insert(self, sql, txn, ai):
        raise NotImplementedError

    def migrate(self, dir, model, stdout=None):  # pragma: no cover
        v2f = {}
        for f in sorted(os.listdir(dir)):
            m = self.RE_MIGRATE_SQL.match(f)
            if m:
                ver = m.group(1)
                ver in v2f and die('migrate version %r is duplicated' % ver)
                v2f[ver] = f

        try:
            with self.txn_do():
                vers = set(model.query().pluck('version'))
        except Exception:
            vers = set()

        for ver, f in v2f.items():
            if ver not in vers:
                with open(dir + '/' + f) as fh, self.txn_do():
                    for sql in fh.read().split(";\n"):
                        sql = sql.strip()
                        if sql:
                            stdout and print(sql + ';')
                            self.execute(sql)
                    stdout and print('')
                    model(version=ver).insert()

    def pluck(self, sql, vs=()):
        csr = self.execute(sql, vs, tuple)
        return Cursor0(csr) if len(csr.description) == 1 else csr

    def quote(self, v):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def rollback(self):
        self._con_x('rollback')

    def txn_do(self):
        return Transaction(self)


class Transaction:
    __slots__ = ('db', 'sp')

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        db = self.db
        sp = self.sp = db.txn_depth
        if sp:
            db.execute(f'SAVEPOINT p{sp}')
            db.txn_depth = sp + 1
        else:
            db.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        db = self.db
        if self.sp >= db.txn_depth:
            pass
        elif not self.sp:
            db.rollback() if exc_type else db.commit()
        else:
            sp = db.txn_depth = self.sp
            db.execute(f'ROLLBACK TO SAVEPOINT p{sp}' if exc_type else
                       f'RELEASE SAVEPOINT p{sp}')

    def rollback(self):
        db = self.db
        if not self.sp:
            db.rollback()
            db.begin()
        else:
            sp = db.txn_depth = self.sp
            db.execute(f'ROLLBACK TO SAVEPOINT p{sp}')
            db.execute(f'SAVEPOINT p{sp}')
            db.txn_depth = sp + 1
