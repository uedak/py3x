from . import SQL, die
from .model import _attr_in_db
from ..utils import cached_property
import re


class Alias():
    __slots__ = ('model', 'as_')

    def __init__(self, model, as_):
        self.model = model
        self.as_ = as_

    def __getattr__(self, k):
        k in self.model.COLUMNS or die.no_attr(self.model, k)
        t = self.as_
        return SQL(f'{t}.{k}' if t else k)


class Relation:
    class Accessor:
        def __init__(self, r):
            self.rel = r

        def __get__(self, obj, cls=None):
            if obj is None:
                return self.rel

            r = self.rel
            d = obj.__dict__
            k = r.name
            return d[k] if k in d else r._get_(obj, d)

        def __set__(self, obj, x):
            self.rel._set_(obj, x)

    RE_JOIN = re.compile(r't2\.(\w+) *= *t1\.(\w+)')

    def __repr__(self):
        return f'{self.model.__name__}.{self.name}'

    @cached_property
    def _query(self):
        m = self.rel_model
        w, o = self._args[1:]
        if isinstance(o, str):
            o = (o,)
        b2s = tuple(c.name for c in (self.model.BELONGINGS.get(m) or ()))

        if callable(w):
            q = (lambda t, r: w(m.as_(t), r)) if not o else \
                (lambda t, r: w(m.as_(t), r).order_by(*o))

            ws = str(q('t2', Alias(self.model, 't1'))
                     .where_sql(verbose=True, wrap=False))
            js = self.RE_JOIN.findall(ws)
            j0 = next((j for j in js if j[0] in b2s), None)
            self.reverse_b2 = j0 and m.COLUMNS[j0[0]].as_
            self.simple_joins = len(js) == len(ws.split(' AND ')) and js
            return q

        if w is None:
            len(b2s or die.no_b2(m, '')) == 1 or die.amb_rel(self)
            w = b2s[0]
        else:
            w in b2s or die.no_b2(m, w)

        pk = self.model.PRIMARY_KEY[0]
        q = (lambda t, r: m.as_(t).where(**{w: getattr(r, pk)})) if not o else\
            (lambda t, r: m.as_(t).where(**{w: getattr(r, pk)}).order_by(*o))

        self.reverse_b2 = m.COLUMNS[w].as_
        self.simple_joins = ((w, pk),)
        return q

    def after_def(self, model, name):
        self.model = model
        self.name = name
        self.form2py = None
        setattr(model, name, self.Accessor(self))

    def as_(self, as_):
        return self.query(None, as_)

    @cached_property
    def join_key(self):
        return self.name

    def on(self, *args, **kw):
        rel = Relation()
        for k in ('model', 'name', 'rel_model', 'join_key', 'reverse_b2'):
            setattr(rel, k, getattr(self, k))
        rel.query = lambda r, as_: self.query(r, as_).where(*args, **kw)
        return rel

    def query(self, r=None, as_=None):
        if r is None or isinstance(r, str):
            m1 = self.model
            t1 = r or m1.DB_TABLE_AS
            sjs = self.simple_joins
            if sjs:
                m2 = self.rel_model
                t2 = m2.DB_TABLE_AS if as_ is None else as_
                if t2 == t1:
                    t2 = ''
                p2 = t2 and t2 + '.'
                return m2.as_(t2).where(SQL(' AND '.join(
                    f'{p2}{c2} = {t1}.{c1}' for c2, c1 in sjs)))
            r = Alias(m1, t1)
        return self._query(as_, r)

    @cached_property
    def rel_model(self):
        return self.model.MODELS[self._args[0]]

    @cached_property
    def reverse_b2(self):
        self._query
        return self.__dict__['reverse_b2']

    @cached_property
    def simple_joins(self):
        self._query
        return self.__dict__['simple_joins']


class BelongsTo(Relation):
    def __init__(self, fk):
        self.fk = fk

    def _get_(self, obj, d):
        fk = self.fk
        k = fk.name
        v = d[k] if k in d else \
            _attr_in_db(None, obj, None, fk) if '.k2i' in d else None
        if v is not None:
            m = self.rel_model
            r = d[self.name] = m.find_by(**{m.PRIMARY_KEY[0]: v})
            return r

    def _set_(self, obj, r):
        fk = self.fk
        d = obj.__dict__
        if r is None:
            d[self.name] = d[fk.name] = None
            return

        isinstance(r, fk.belongs_to) or die.type(r, fk.belongs_to)
        d[self.name] = r

        k = fk.name
        v = getattr(r, fk.b2pk)
        if v is not None:
            d[k] = v
        elif k in d:
            del d[k]

    def after_def(self, model, name):
        super().after_def(model, name)
        fk = self.fk
        m = self.rel_model = fk.belongs_to
        pk = m.PRIMARY_KEY[0]
        k = fk.name
        self._query = lambda t, r: m.as_(t).where(**{pk: getattr(r, k)})
        self.reverse_b2 = False
        self.simple_joins = ((pk, k),)


class HasMany(Relation):
    def __init__(self, rel_name, where=None, *, order_by=None, cache=True):
        self._args = (rel_name, where, order_by)
        self.cache = cache
        self.join_key = None

    def _get_(self, obj, d):
        rs = d[self.name] = self.query(obj).cache(self.cache)
        return rs

    def _set_(self, obj, rs):
        if rs:
            m = self.rel_model
            rb2 = self.reverse_b2
        for r in rs:
            isinstance(r, m) or die.type(r, m)
            rb2 and setattr(r, rb2, obj)
        obj.__dict__[self.name] = rs


class HasOne(Relation):
    def __init__(self, rel_name, where=None):
        self._args = (rel_name, where, None)

    def _get_(self, obj, d):
        if self.fk:
            return BelongsTo._get_(self, obj, d)
        r = d[self.name] = self.query(obj).peek()
        return r

    def _set_(self, obj, r):
        if r is not None:
            isinstance(r, self.rel_model) or die.type(r, self.rel_model)
            rb2 = self.reverse_b2
            rb2 and setattr(r, rb2, obj)
        obj.__dict__[self.name] = r

    @cached_property
    def fk(self):
        sjs = self.simple_joins
        if sjs and len(sjs) == 1:
            c1, c2 = sjs[0]
            if (c1,) == self.rel_model.PRIMARY_KEY:
                return self.model.COLUMNS[c2]
