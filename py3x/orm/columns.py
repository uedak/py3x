from . import die
from ..utils import _NX, Date, DateTime, IRANGE, SRE_Pattern, XEnum, \
    cached_class_property, qw
from .model import _attr_in_db
from math import log2
import re
__all__ = qw('INT BELONGS_TO BOOL DATE DATETIME VARCHAR TEXT BLOB NOW TODAY')


class Getter:
    def __init__(self, col):
        self.col = col

    __get__ = _attr_in_db


class Column:
    Accessor = Getter
    FREQ_ATTRS = qw('PY_TYPE auto_increment db2py default form2py '
                    'nn_on_create py2db restore_attr strict txn_value')
    INIT_KWS = {*qw(
        'auto_increment choices default index label multiple null primary_key '
        'strict type_suffix unique validate')}
    auto_increment = belongs_to = choices = db2py = default = index = label = \
        multiple = nn_on_create = null = primary_key = py2db = restore_attr = \
        strict = txn_value = type_suffix = unique = validate = None

    @property
    def DB_TYPE(self):
        return self.__class__.__name__

    def __init__(self, **kw):
        ik = self.INIT_KWS
        for k, v in kw.items():
            setattr(self, k, v) if k in ik else die.bad_kw('__init__()', k)

    def __repr__(self):
        return f'{self.model.__name__}.{self.name}'

    def after_def(self, model, name):
        self.model = model
        self.name = name
        vs = self.validate
        vs = self.validate = {**vs} if vs else {}
        vsd = vs.setdefault
        es = model.Errors
        self.null or vsd(
            'nn', es.UNSELECTED if self.belongs_to or self.choices else True)
        self.choices and self.def_choices(model, vsd)
        self.after_def_(model, vsd)

        for k, v in vs.items():
            oa = getattr(es, 'validate_' + k).__annotations__.get('o')
            if oa is SRE_Pattern and isinstance(v, str):
                vs[k] = re.compile(v)

        setattr(model, name, self.Accessor(self))
        for k in self.FREQ_ATTRS:
            setattr(self, k, getattr(self, k))

    def after_def_(self, model, vsd):  # pragma: no cover
        pass

    def db_type(self):
        return self.DB_TYPE

    def ddl_sqls(self):
        ss = [self.name, self.db_type()]
        self.type_suffix and ss.append(self.type_suffix)
        self.null or ss.append('NOT NULL')
        self.primary_key and ss.append('PRIMARY KEY')
        return ss

    def def_choices(self, model, vsd):
        name = self.name
        cs = self.choices
        enum = isinstance(cs, XEnum)
        if enum:
            cs.__module__ = model.__module__
            cs.__name__ = f'{model.__name__}.{name.upper()}'
            setattr(model, name.upper(), cs)
            if self.multiple:
                def choices(r, k='label'):
                    return [getattr(v, k) for v in (getattr(r, name) or ())
                            if v in cs]
            else:
                def choice(r, k='label'):
                    v = getattr(r, name)
                    return getattr(v, k) if v in cs else None
            v2x = cs.__getattr__('.v2x')
        else:
            if self.multiple:
                def choices(r):
                    return [cs[v] for v in (getattr(r, name) or ()) if v in cs]
            else:
                def choice(r):
                    return cs.get(getattr(r, name))
            v2x = {}
            v2s = model.Util.val2str
            for c in cs:
                v2x[c] = v2x[v2s(c)] = c

        if self.multiple:
            self.db2py is None or die.incompo(self, 'multiple', 'db2py')
            self.py2db is None or die.incompo(self, 'multiple', 'py2db')

            bits = 0
            for c in cs:
                bits |= c and isinstance(c, int) and (bits & c) == 0 and \
                    log2(c).is_integer() and c or die.bad_bit(self, '', c)

            self.db2py = lambda v: v if v is None else [] if v == 0 else \
                [c for c in cs if c & v]
            self.form2py = lambda vs: [v2x.get(v, v) for v in vs if v != ''] \
                if isinstance(vs, list) else vs
            self.py2db = lambda vs: vs if vs is None else sum(vs)

            d0 = self.default
            if isinstance(d0, int):
                (d0 | bits) == bits or die.bad_bit(self, '.default', d0)
                d0 = self.db2py(d0)
                self.default = lambda r: [*d0]

            vsd('type', list)
            vsd('choices', cs)
            setattr(model, name + '_choices', choices)

        elif enum:
            self.db2py is None or die.incompo(self, 'XEnum', 'db2py')
            self.db2py = lambda v: v if v is None else cs.get(v, v)
            self.form2py = lambda v: None if v == '' else cs.get(v, v)
            if self.default is not None and not callable(self.default):
                self.default = cs[self.default]
            vsd('type', cs.__getattr__('.X'))
            vsd('choice', cs)
            setattr(model, name + '_choice', choice)

        else:
            vsd('choice', cs)
            setattr(model, name + '_choice', choice)

    def form2py(self, v):
        raise NotImplementedError


class _SERIAL:
    auto_increment = primary_key = True

    @property
    def DB_TYPE(self):
        return self.__class__.__base__.__name__

    def ddl_sqls(self):
        ss = [self.name, self.__class__.__name__]
        self.primary_key and ss.append('PRIMARY KEY')
        return ss


class BLOB(Column):
    PY_TYPE = bytes
    size = None

    def after_def_(self, model, vsd):
        self.size and vsd('len', (None, self.size))

    def form2py(self, v):
        return v.encode() if isinstance(v, str) else v


class BOOL(Column):
    PY_TYPE = bool

    def after_def_(self, model, vsd):
        self.str2bool = model.Util.str2bool
        vsd('type', bool)

    def db2py(self, v):
        return v if v is None else bool(v)

    def form2py(self, v):
        return None if v == '' else self.str2bool(v, self.strict)


class DATE(Column):
    PY_TYPE = Date

    def after_def_(self, model, vsd):
        self.str2date = model.Util.str2date
        vsd('type', Date)

    def form2py(self, v):
        return None if v == '' else self.str2date(v, self.strict)


class DATETIME(Column):
    PY_TYPE = DateTime

    def after_def_(self, model, vsd):
        self.str2datetime = model.Util.str2datetime
        vsd('type', DateTime)

    def form2py(self, v):
        return None if v == '' else self.str2datetime(v, self.strict)


class INT(Column):
    PY_TYPE = int
    size = 4

    def after_def_(self, model, vsd):
        self.str2int = model.Util.str2int
        vsd('type', int)
        vsd('range', self.default_range())

    def default_range(self):
        return IRANGE[self.size]

    def form2py(self, v):
        return None if v == '' else self.str2int(v, self.strict)


def _set_b2(self, obj, v, col=None):
    if col is None:
        col = self.col
    as_ = col.as_
    d = obj.__dict__
    if as_ in d:
        if v is None:
            del d[as_]
        else:
            r = d[as_]
            if r is None or getattr(r, col.b2pk) != v:
                del d[as_]
    d[col.name] = v


class BELONGS_TO(Column):
    class Accessor(Getter):
        def __get__(self, obj, cls=None):
            if obj is None:
                return self.col

            col = self.col
            k = col.name
            d = obj.__dict__
            if k in d:
                return d[k]
            if '.k2i' in d:
                return _attr_in_db(None, obj, None, col)
            as_ = col.as_
            if as_ in d:
                r = d[as_]
                if r is not None:
                    return getattr(r, col.b2pk)

        __set__ = _set_b2

    INIT_KWS = {*Column.INIT_KWS, 'as_', 'foreign_key'}
    as_ = None
    db2py = foreign_key = index = True

    @cached_class_property
    def BelongsTo(cls):
        from .relations import BelongsTo
        return BelongsTo

    def __init__(self, b2, **kw):
        len(b2.PRIMARY_KEY) == 1 or die(f'{b2} has no single primary key')
        super().__init__(**kw)
        self.belongs_to = b2

    def after_def_(self, model, vsd):
        b2 = self.belongs_to
        self.b2pk = b2.PRIMARY_KEY[0]
        c = b2.COLUMNS[self.b2pk]
        for k in ('PY_TYPE', 'db_type', 'form2py'):
            setattr(self, k, getattr(c, k))
        vsd('type', self.PY_TYPE)
        if c.db2py is None:
            c.db2py = True

        if self.as_ is None:
            self.as_ = model.belongs_to_x_as(b2, self.name) or \
                die('unable to resolve column: %r' % self.name)
        rel = model.ATTRS[self.as_] = self.BelongsTo(self)
        rel.after_def(model, self.as_)

        m2cs = b2.BELONGINGS
        cs = m2cs.get(model)
        if cs is None:
            cs = m2cs[model] = []
        cs.append(self)

    def nn_on_create(self, d):
        as_ = self.as_
        return as_ not in d or d[as_] is None

    def restore_attr(self, d, k, obj):
        self.as_ in d and _set_b2(
            None, obj, _attr_in_db(None, obj, None, self, None), self)
        del d[k]

    def txn_value(self, d):
        as_ = self.as_
        r = d[as_] if as_ in d else None
        return _NX if r is None else getattr(r, self.b2pk)


class TEXT(Column):
    PY_TYPE = str
    size = None

    def after_def_(self, model, vsd):
        vsd('chars', model.Util.RE_CHARS)
        self.size and vsd('len', (None, self.size))

    def form2py(self, v):
        return v


class VARCHAR(Column):
    PY_TYPE = str

    def __init__(self, size=255, *, blank=None, **kw):
        self.size = size
        self.blank = blank
        super().__init__(**kw)

    def after_def_(self, model, vsd):
        if self.blank is None:
            self.blank = self.name not in model.UNIQUE_COLUMNS
        vsd('chars', model.Util.RE_CHARS_INLINE)
        vsd('len', (None, self.size))

    def db_type(self):
        return f'{self.DB_TYPE}({self.size})'

    def form2py(self, v):
        return None if v == '' and not self.blank else v
