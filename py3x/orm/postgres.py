from ..utils import cached_class_property, cached_property, include
from . import columns, database, query
from .columns import BELONGS_TO, BOOL, DATE, INT, TEXT, VARCHAR  # noqa
from .model import Model, NOW, TODAY  # noqa
from .relations import HasMany, HasOne  # noqa


class BIGINT(INT):
    size = 8


class BIGSERIAL(BIGINT):
    include(columns._SERIAL)


class BYTEA(columns.BLOB):
    def db2py(self, v):
        return v if v is None else v.tobytes()


class CITEXT(TEXT):
    pass


class SERIAL(INT):
    include(columns._SERIAL)


class SMALLINT(INT):
    size = 2


class TIMESTAMP(columns.DATETIME):
    pass


class PgQuery(query.Query):
    __slots__ = ()
    BORDER = "'|' AS \"|\""

    def _build_join(self, ss, vs, wp, t2j):
        if not t2j:
            pass
        elif ss[0] == 'DELETE':
            ts = ', '.join(f'{j[1].DB_TABLE} {t}' for t, j in t2j.items())
            ss.append(f"USING {ts}")
        else:
            super()._build_join(ss, vs, wp, t2j)

    def _build_where(self, ss, vs, wp, wss):
        t2j = ss and ss[0] == 'DELETE' and self.kw.get('join')
        if not t2j:
            wss and super()._build_where(ss, vs, wp, wss)
            return

        for i, j in enumerate(t2j.values()):
            _s, _vs = j[2]  # on
            ss.append('AND' if i else 'WHERE')
            ss.append(f'({_s})')
            _vs and vs.extend(_vs)

        if wss:
            ss.append('AND')
            i = len(ss)
            for ws in wss[0]:
                self._add2(ss, vs, wp, None, *ws)
            self._add2(ss, vs, wp, None, *wss[1])
            ss[i] = '(' + ss[i]
            ss[-1] += ')'

    def delete(self):
        return self._clone({**self.kw, 'type': 'DELETE'})


class Database(database.Database):
    Query = PgQuery

    @cached_class_property
    def STRING_TYPES(cls):
        from psycopg2.extensions import AsIs, new_type, register_adapter
        i2t = {}
        for i, dt in ((1082, cls.Util.Date), (1114, cls.Util.DateTime)):
            register_adapter(dt, lambda x: AsIs(f"'{x}'"))
            i2t[i] = new_type((i,), dt.__name__, dt.from_db)
        return i2t

    def connect(self, **kw):
        from psycopg2 import connect
        con = connect(**kw)
        con.string_types.update(self.STRING_TYPES)
        return con

    def execute_insert(self, sql, vs, ai):
        return self.execute(f'{sql} RETURNING {ai}', vs, 1)[0]

    @cached_property
    def quote(self):
        m = self._con.cursor().mogrify
        return lambda v: m('%s', (v,)).decode()

    def reset(self):
        x = self.execute
        ts = 'SELECT tablename FROM pg_tables WHERE tableowner = current_user'
        ps = (
            "SELECT proname FROM pg_proc p "
            "JOIN pg_user u ON u.usesysid = p.proowner "
            "JOIN pg_language l ON l.oid = p.prolang "
            "WHERE u.usename = current_user AND l.lanname = 'plpgsql'")
        with self.txn_do():
            for t, in reversed(tuple(x(ts))):
                x(f'DROP TABLE {t} CASCADE')
            for p, in x(ps):
                x(f'DROP FUNCTION {p} CASCADE')
