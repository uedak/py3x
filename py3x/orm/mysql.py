from ..utils import IRANGE, IRANGE_U, \
    cached_class_property, cached_property, include
from . import columns, database
from .columns import BELONGS_TO, BOOL, DATE, DATETIME, VARCHAR  # noqa
from .model import Model, NOW, TODAY  # noqa
from .relations import HasMany, HasOne  # noqa
import zlib


class _64k:
    def __init__(self, size=65535, **kw):
        self.size = size
        self.__class__.__base__.__init__(self, **kw)

    def db_type(self):
        t, s = self.DB_TYPE, self.size
        return t if s == 65535 else f'{t}({s})'


class BLOB(columns.BLOB):
    include(_64k)


class MEDIUMBLOB(columns.BLOB):
    size = 2 ** 24 - 1


class LONGBLOB(columns.BLOB):
    size = 2 ** 32 - 1


class INT(columns.INT):
    INIT_KWS = {*columns.INT.INIT_KWS, 'unsigned'}
    unsigned = None

    def __init__(self, **kw):
        super().__init__(**kw)
        if self.primary_key and self.auto_increment is None and \
           not self.belongs_to:
            self.auto_increment = True

    def db_type(self):
        t = self.DB_TYPE
        return t + ' UNSIGNED' if self.unsigned else t

    def ddl_sqls(self):
        ss = super().ddl_sqls()
        self.auto_increment and ss.append('AUTO_INCREMENT')
        return ss

    def default_range(self):
        return (IRANGE_U if self.unsigned else IRANGE)[self.size]


class TINYINT(INT):
    size = 1


class SMALLINT(INT):
    size = 2


class BIGINT(INT):
    size = 8


class SERIAL(BIGINT):
    include(columns._SERIAL)
    unsigned = True


class TEXT(columns.TEXT):
    include(_64k)


class MEDIUMTEXT(columns.TEXT):
    size = 2 ** 24 - 1


class LONGTEXT(columns.TEXT):
    size = 2 ** 32 - 1


class _Z:
    def db2py(self, v):
        if v is not None:
            return zlib.decompress(v[4:]).decode()

    def py2db(self, v):
        if v is not None:
            v = v.encode()
            return len(v).to_bytes(4, 'little') + zlib.compress(v, 6)


class ZTEXT(TEXT):
    include(_Z)
    DB_TYPE = 'BLOB'


class MEDIUMZTEXT(MEDIUMTEXT):
    include(_Z)
    DB_TYPE = 'MEDIUMBLOB'


class LONGZTEXT(LONGTEXT):
    include(_Z)
    DB_TYPE = 'LONGBLOB'


class Database(database.Database):
    @cached_class_property
    def CONV(cls):
        from MySQLdb.constants import FIELD_TYPE
        from MySQLdb.converters import conversions
        c = conversions.copy()
        c[FIELD_TYPE.DATE] = cls.Util.Date.from_db
        c[FIELD_TYPE.DATETIME] = cls.Util.DateTime.from_db
        c[FIELD_TYPE.TIMESTAMP] = cls.Util.DateTime.from_db
        return c

    def connect(self, **kw):
        kw.setdefault('conv', self.CONV)
        from MySQLdb import connect
        return connect(**kw)

    def execute_insert(self, sql, vs, ai):
        return self.execute(sql, vs, tuple).lastrowid

    @cached_property
    def quote(self):
        e = self._con.escape
        lt = self._con.literal

        def quote(v):
            if isinstance(v, bytes):
                return '0x' + v.hex()
            x = e(v.encode()) if isinstance(v, str) else lt(v)
            return x.decode() if isinstance(x, bytes) else x

        return quote

    def reset(self):
        x = self.execute
        x('SET foreign_key_checks=0')
        for s in x('SHOW TABLE STATUS'):
            x('DROP %s %s' % (s[-1] or 'TABLE', s[0]))
        db = self.con_kw['database']
        for s in x('SHOW FUNCTION STATUS WHERE Db = %s', (db,)):
            x('DROP %s %s' % (s[2], s[1]))
        x('SET foreign_key_checks=1')
