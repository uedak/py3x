from ..utils import die
from types import GeneratorType


class die(die):
    AMB_COL = 'column %r is ambiguous'
    AMB_REL = 'relation %r is ambiguous'
    BAD_BIT = '%r%s: invalid bit %r'
    BAD_COL = 'unknown column: %r'
    BAD_REL = 'unknown relation: %r'
    INCOMPO = '%r: %s and %s are incompossible'
    NO_B2 = '%r has no BELONGS_TO: %s'
    NO_PAGE = 'page() not called'
    NO_PK = 'no primary key on %r'
    NO_TA = 'no table alias for %r'

    @classmethod
    def col_nld(cls, k):
        raise ColumnNotLoaded(k)


class BulkLoader():
    def __init__(self, db, table, columns, *, per=None, suffix=''):
        self.db = db
        self.per = per
        self.count = 0
        self._sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n",
            suffix and ("\n" + suffix))
        d = isinstance(columns, dict)
        cs = ', '.join(columns.values() if d else ('%s',) * len(columns))
        self._cs = (f'({cs})',)
        self._nc = cs.count('%s') if d else len(columns)
        self._nr = 0
        self._vs = []

    def _build(self):
        xs = ",\n".join(self._cs * self._nr)
        s1, s2 = self._sql
        return f'{s1}{xs}{s2}' if s2 else (s1 + xs)

    def add(self, *vs):
        len(vs) == self._nc or die.n_args('add()', self._nc, 'p', len(vs))
        self._nr += 1
        self._vs.extend(vs)
        self.per and self._nr >= self.per and self.execute()
        return self

    def clear(self):
        self._nr = 0
        self._vs.clear()
        return self

    def execute(self):
        if self._nr:
            csr = self.db.execute(self._build(), self._vs)
            self.count += self._nr
            self.clear()
            return csr

    def extend(self, vss):
        for vs in vss:
            self.add(*vs)
        return self

    def sql(self):
        return SQL(self._build(), *self._vs)


class ColumnNotLoaded(Exception):
    pass


class RecordNotFound(Exception):
    pass


class SQL:
    __slots__ = ('_t',)

    def __bool__(self):
        return bool(self._t[0])

    def __eq__(self, x):
        return isinstance(x, SQL) and x._t == self._t

    def __hash__(self):
        return hash(self._t)

    def __init__(self, s, *vs):
        self._t = (s, vs)

    def __iter__(self):
        return iter(self._t)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, [
            self._t[0], *self._t[1]].__repr__()[1:-1])

    def __str__(self):
        return self._t[0]

    def as_(self, as_):
        return self.wrap(f'(%s) AS {as_}')

    def wrap(self, f):
        sql = object.__new__(self.__class__)
        t = self._t
        sql._t = (f % t[0], t[1])
        return sql


EXISTS = SQL.EXISTS = lambda q: q.exists_sql()


class Operator(SQL):
    __slots__ = ()


BETWEEN = SQL.BETWEEN = lambda v1, v2: Operator('BETWEEN %s AND %s', v1, v2)
GE = SQL.GE = lambda v: Operator('>= %s', v)
GT = SQL.GT = lambda v: Operator('> %s', v)
LE = SQL.LE = lambda v: Operator('<= %s', v)
LIKE = SQL.LIKE = lambda v: Operator('LIKE %s', v)
LT = SQL.LT = lambda v: Operator('< %s', v)
NE = SQL.NE = lambda v: Operator('!= %s', v)


def IN(*vs):
    if len(vs) == 1 and type(vs[0]) is GeneratorType:
        vs = tuple(vs[0])
    if len(vs) == 1 and isinstance(vs[0], SQL):
        s, vs = vs[0]._t
        s = f'IN ({s})'
    else:
        s = 'IN (NULL)' if not vs else f"IN ({', '.join(('%s',) * len(vs))})"

    op = object.__new__(Operator)
    op._t = (s, vs)
    return op


SQL.IN = IN


def NOT(x):
    if x is None:
        s = 'IS NOT NULL'
        vs = ()
    elif isinstance(x, SQL):
        s, vs = x._t
        s = f'NOT {s}'
    else:
        s = '!= %s'
        vs = (x,)

    op = object.__new__(Operator)
    op._t = (s, vs)
    return op


SQL.NOT = NOT
