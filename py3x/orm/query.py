from . import Operator, SQL, die
from ..utils import _NX, cached_class_property, include, qw, repr_kw, try_
from .model import ModelClass
from .relations import Relation
import re


class Paginate:
    @property
    def current_page(self):
        return (self.kw.get('page') or die.no_page())[0]

    @property
    def first_of_page(self):
        pp = self.per_page
        return self.total_count and pp * (self.current_page - 1) + 1

    @property
    def is_first_page(self):
        return self.current_page == 1

    @property
    def is_last_page(self):
        return self.current_page >= self.last_page

    @property
    def last_of_page(self):
        return self.total_count if self.is_last_page else \
            self.per_page * self.current_page

    @property
    def last_page(self):
        pp = self.per_page
        tc = self.total_count
        return int((tc - 1) / pp) + 1 if tc else 1

    def page(self, current_page, per_page=None):
        m = self.model
        cp = current_page and try_(lambda: int(current_page)) or 1
        cp = max(cp, 1)
        pp = per_page and try_(lambda: int(per_page)) or m.PER_PAGE
        pp = min(max(pp, 1), m.MAX_PER_PAGE)
        return self._clone({
            **self.kw, 'limit': pp, 'offset': pp * (cp - 1), 'page': (cp, pp),
        }).cache(True)

    def pages(self, n=10):
        cp = self.current_page
        i = cp - int((n - 1) / 2)
        j = cp + int(n / 2)
        lp = self.last_page
        if i < 1:
            j += 1 - i
            i = 1
        if j > lp:
            i -= j - lp
            j = lp
        return range(1 if i < 1 else i, (lp if j > lp else j) + 1)

    @property
    def per_page(self):
        return (self.kw.get('page') or die.no_page())[1]


_NCNL = type('Query(no cache, no limit)', (), {})()


def _always(fn):
    fn._always = True
    return fn


class Query:
    include(Paginate)
    __slots__ = qw('model as_ kw _cache _total_count')
    BORDER = "'|'"
    RE_ORDER_BY = re.compile(
        r'\A(?:(\w+)\.)?((\w+)(?: +(?:ASC|DESC))?)\Z', re.IGNORECASE)
    RE_TA = re.compile(r'[A-Za-z_][0-9A-Za-z_]*')
    SELECT_ = qw('from join where group_by having')
    SELECT = ('select', *SELECT_, *qw('order_by limit offset for_update'))
    UPDATE = qw('update join set where order_by limit')
    DELETE = qw('delete from join where order_by limit')
    WHERE_ = ('where',)

    @cached_class_property
    def BUILD_CACHE(cls):
        return {}

    @classmethod
    def _assert_ta(cls, ta):
        if not (isinstance(ta, str) and cls.RE_TA.fullmatch(ta)):
            raise TypeError('invalid table alias: %r' % ta)

    def __bool__(self):
        if getattr(self, '_cache', False) is False:
            raise TypeError('evaluation of Query(no cache) is deprecated')
        return len(self) > 0

    def __getitem__(self, k):
        c = self.cache()
        return (_NCNL if c is False else c)[k]

    def __init__(self, model, as_, **kw):
        self.model = model
        self.as_ = as_
        self.kw = kw

    def __iter__(self):
        c = self.cache()
        return self._iter_rows() if c is False else iter(c)

    def __len__(self):
        c = self.cache()
        return len(_NCNL if c is False else c)

    def __repr__(self, verbose: bool = False):
        return '%s(%r, %r%s)' % (
            self.__class__.__name__, self.model, self.as_,
            '' if not self.kw else ', ...' if not verbose else
            f', {repr_kw(self.kw)}')

    def _add2(self, ss, vs, wp, pfx, *xs):
        ssa = ss.append
        for x in xs:
            t = x.__class__
            if t is str:
                ssa(f'{pfx}.{x}' if wp and pfx else x)
            elif t is tuple:
                s = f'{x[0]}.{x[1]}' if wp and x[0] else x[1]
                if len(x) == 3:
                    v = x[2]
                    if isinstance(v, SQL):
                        _s, _vs = v._t
                        s += _s
                        _vs and vs.extend(_vs)
                    else:
                        vs.append(v)
                ssa(s)
            elif isinstance(x, SQL):
                _s, _vs = x._t
                ssa(_s)
                _vs and vs.extend(_vs)
            else:
                die(TypeError(x))
        return ss

    def _build(self, t, wp=None):
        kfas = self.BUILD_CACHE.get(t)
        if not kfas:
            cls = self.__class__
            kfs = ((k, getattr(cls, '_build_' + k)) for k in getattr(cls, t))
            kfas = cls.BUILD_CACHE[t] = tuple(
                (k, f, getattr(f, '_always', None)) for k, f in kfs)

        ss = []
        vs = []
        kw = self.kw
        if wp is None:
            wp = 'join' in kw
        for k, f, a in kfas:
            i = k in kw or None
            (a or i) and f(self, ss, vs, wp, i and kw[k])
        return SQL(' '.join(ss), *vs)

    @_always
    def _build_delete(self, ss, vs, wp, ts):
        ts = ts and not self.kw.get('order_by') and ', '.join(ts)  # mysql 8
        ss.append('DELETE ' + ts if ts else 'DELETE')

    def _build_for_update(self, ss, vs, wp, x):
        x and ss.append('FOR UPDATE')

    @_always
    def _build_from(self, ss, vs, wp, x):
        self._add2(ss, vs, wp, None, 'FROM', x or self._db_table())

    def _build_group_by(self, ss, vs, wp, x):
        x and self._add2(ss, vs, wp, None, 'GROUP BY', x)

    def _build_having(self, ss, vs, wp, x):
        x and self._add2(ss, vs, wp, None, 'HAVING', x)

    def _build_join(self, ss, vs, wp, t2j):
        if t2j:
            for t, j in t2j.items():
                _s, _vs = j[2]._t
                ss.append(f'{j[0]} {j[1].DB_TABLE} {t} ON {_s}')
                _vs and vs.extend(_vs)

    def _build_limit(self, ss, vs, wp, x):
        x is None or (ss.append('LIMIT %s'), vs.append(x))

    def _build_offset(self, ss, vs, wp, x):
        x and (ss.append('OFFSET %s'), vs.append(x))

    def _build_order_by(self, ss, vs, wp, xs):
        xs and ss.append('ORDER BY ' + ', '.join(
            self._add2([], vs, wp, None, *xs)))

    @_always
    def _build_select(self, ss, vs, wp, t2xs):
        _ss = []
        if t2xs:
            for t, xs in t2xs.items():
                if '*' in xs:
                    self._add2(_ss, vs, wp, t,
                               '*', *(x for x, v in xs.items() if v))
                else:
                    self._add2(_ss, vs, wp, t, *xs)
                self._xs_len(xs) is None and _ss.append(self.BORDER)
            _ss[-1] == self.BORDER and _ss.pop()
        else:
            self._add2(_ss, vs, wp, self.as_, *self.model.DB_SELECT)
        ss.append('SELECT ' + ', '.join(_ss))

    @_always
    def _build_set(self, ss, vs, wp, xs):
        ss.append('SET ' + ', '.join(self._add2([], vs, wp, None, *xs)))

    @_always
    def _build_update(self, ss, vs, wp, x):
        ss.append('UPDATE ' + self._db_table())

    def _build_where(self, ss, vs, wp, wss):
        if wss:
            ss.append('WHERE')
            for ws in wss[0]:
                self._add2(ss, vs, wp, None, *ws)
            self._add2(ss, vs, wp, None, *wss[1])

    def _clone(self, kw, *dks):
        q = object.__new__(self.__class__)
        q.model = self.model
        q.as_ = self.as_
        q.kw = kw
        for k in dks:
            if k in kw:
                del kw[k]
        return q

    def _clone_setter(k, t, *, qn=None):
        def m(self, v):
            return self._clone({**self.kw}, k) if v is None else \
                self._clone({**self.kw, k: v}) if isinstance(v, t) else \
                die.type(v, t)
        m.__qualname__ = 'Query.' + (qn or k)
        return m

    def _db_table(self):
        t = self.model.DB_TABLE
        as_ = self.as_
        return f'{t} {as_}' if as_ else t

    def _fetch_info(self, m, hs, dsi, fc, xs, id2r=None):
        npk = len(m.PRIMARY_KEY)
        npk = npk and hs[:npk] == m.PRIMARY_KEY and npk
        wfc = npk and fc is not None
        if wfc and xs is not True:
            cs = m.COLUMNS if '*' in xs else ()
            wfc = all((s in xs or s in cs) for s in m.DB_SELECT)
        return (m.instantiate, dsi(hs), npk,
                m.DB_TABLE if wfc else None, {} if id2r and npk else None)

    def _fetch_infos(self, t2xs, hs, dsi, fc, peek):
        t2j = self.kw['join'].get
        t2ijx = {t: (ri, t2j(t), xs)
                 for ri, (t, xs) in enumerate(t2xs.items())}
        fis = []
        jkis = []
        i1 = 0
        for t, (ri, j, xs) in t2ijx.items():
            n = self._xs_len(xs)
            i2 = hs.index('|', i1) if n is None else i1 + n
            m = j[1] if j else self.model
            fis.append((ri, i1, i2, *self._fetch_info(
                m, hs[i1:i2], dsi, fc, xs, ri and not peek)))
            i1 = i2 + 1 if n is None else i2

            if j and j[3] in t2ijx:
                _t, jk, rb2 = j[3:6]
                _i = t2ijx[_t][0]
                ri and jk and jkis.append((_i, jk, ri))
                rb2 and jkis.append((ri, rb2, _i))

        return fis, jkis

    def _iter_rows(self, peek=None):
        db = self.model.DB
        fc = peek and db.find_cache
        csr = db.execute(*self._build('SELECT'), tuple)
        t2xs = self.kw.get('select')
        if not t2xs or len(t2xs) == 1:
            t = next(iter(t2xs)) if t2xs else self.as_
            m = self.model if t == self.as_ else self.kw['join'][t][1]
            hs = tuple(d[0] for d in csr.description)
            if fc is None:
                mi, k2i, mt = m.instantiate, db.shared_index(hs), None
            else:
                mi, k2i, npk, mt, id2r = self._fetch_info(
                    m, hs, db.shared_index, fc, t2xs[t] if t2xs else True)
            for vs in csr:
                fck = mt and ((mt, vs[0]) if npk == 1 else (mt, *vs[:npk]))
                yield mi(k2i, vs, fck, fc)
            return

        hs = (*(d[0] for d in csr.description), '|')
        fis, jkis = self._fetch_infos(t2xs, hs, db.shared_index, fc, peek)
        rs = [None] * len(fis)
        for vs in csr:
            for ri, i1, i2, mi, k2i, npk, mt, id2r in fis:
                r = rs[ri] = None
                if vs[i1] is not None:
                    if id2r is not None:
                        id = vs[i1] if npk == 1 else vs[i1:i1 + npk]
                        if id in id2r:
                            r = id2r[id]
                    if r is None:
                        fck = mt and ((mt, vs[i1]) if npk == 1 else
                                      (mt, *vs[i1:i1 + npk]))
                        r = mi(k2i, vs[i1:i2], fck, fc)
                        if id2r is not None:
                            id2r[id] = r
                    rs[ri] = r

            for j, k, i in jkis:
                if rs[j] is not None:
                    rs[j].__dict__[k] = rs[i]

            if rs[0] is not None:
                yield rs[0]

    def _iter_tms(self, k=None):
        if k is None:
            yield (self.as_, self.model)
            if 'join' in self.kw:
                for t, j in self.kw['join'].items():
                    yield (t, j[1])
        elif k == self.as_:
            yield (k, self.model)
        else:
            t2j = self.kw.get('join')
            t2j and k in t2j or die.no_ta(k)
            yield (k, t2j[k][1])

    def _join_rel(self, t, k, m=None):
        tms = tuple(
            tm for tm in self._iter_tms(t) if (m is None or tm[1] == m) and
            isinstance(tm[1].ATTRS.get(k), Relation))
        tms or die.bad_rel(k)
        len(tms) == 1 or die.amb_rel(k)
        t, m = tms[0]
        t or die.no_ta(m)
        return t, m.ATTRS[k]

    def _select(self, t2xs, args, kw):
        as_ = self.as_
        t2j = self.kw.get('join') or ()
        if '*.*' in args:
            args = tuple(x for x in args if x != '*.*')
            for t in (as_, *t2j):
                if t not in kw:
                    kw[t] = '*'

        n1 = len(t2xs) or 1
        xs0 = None
        for t, vs in (((as_, args), *kw.items()) if args else kw.items()):
            m = self.model if t == as_ else \
                t2j[t][1] if t in t2j else die.no_ta(t)
            vs or die(TypeError('no columns for %r' % t))
            cs = m.COLUMNS
            xs = t2xs[t] = xs0 if t == as_ and xs0 is not None else \
                {**t2xs[t]} if t in t2xs else \
                {k: None for k in m.PRIMARY_KEY} if t2xs else {}
            for v in (m.DB_SELECT if vs is True else vs):
                xs[v] = None if v == '*' or v in cs else \
                        True if isinstance(v, SQL) else die.bad_col(v)
            if t == as_:
                xs0 = xs

        if len(t2xs) > n1:
            ts = set()
            for t, xs in t2xs.items():
                j = t2j[t] if t in t2j else None
                cs = (j[1] if j else self.model).COLUMNS
                s1 = next(iter(cs if '*' in xs else xs))
                s1 in cs and not cs[s1].null or die(TypeError(
                    f'{t}.{s1}: first column must be NOT NULL'))

                if j and j[3] in t2xs:
                    _t, jk, rb2 = j[3:]
                    jk and ts.add(t)
                    rb2 and ts.add(_t)
            for i, t in enumerate(t2xs):
                i == 0 or t in ts or die(TypeError('%r is unreachable' % t))

        return self._clone({**self.kw, 'select': t2xs}, 'type')

    def _xs_len(self, xs):
        return None if '*' in xs or any(xs.values()) else len(xs)

    def cache(self, on=_NX):
        if on is False:
            self._cache = False
            return self

        c = getattr(self, '_cache', None)  # None|False|True|[...]
        if on is _NX:  # no args => getter
            if c is None:
                c = 'limit' in self.kw
            if c is True:
                c = self._cache = list(self._iter_rows())
            return c

        on is True or on is None or die.type(on, bool, None)
        if (c is None or c is False) if on else (c is not False):
            self._cache = True  # None => discard cache
        return self

    def count(self, one=1):
        return self.model.DB.execute(*self.count_sql(one), 1)[0]

    def count_sql(self, one=1):
        return self._build('SELECT_').wrap(f'SELECT COUNT({one}) %s')

    def delete(self, *ts):
        ts and all(next(self._iter_tms(t)) for t in ts)
        return self._clone({
            **self.kw, 'type': 'DELETE', 'delete': ts or (self.as_,)})

    def execute(self):
        return self.model.DB.execute(*self.sql(), int)

    def exists(self):
        sql = self._build('SELECT_').wrap('SELECT 1 %s LIMIT 1')
        return bool(self.model.DB.execute(*sql, 1))

    def exists_sql(self):
        return self._build('SELECT_').wrap('EXISTS (SELECT 1 %s)')

    def first(self):
        return self.limit(1).peek()

    def for_update(self, on=True):
        return self._clone({**self.kw, 'for_update': on}) if on else \
            self._clone({**self.kw}, 'for_update')

    from_ = _clone_setter('from', SQL, qn='from_')
    group_by = _clone_setter('group_by', SQL)
    having = _clone_setter('having', SQL)

    def join(self, *args, prefix='JOIN', as_=None, on=None, **kw):
        len(kw) < 2 or die.n_args('join()', '0 or 1', 'k', len(kw))
        na = len(args)
        t2j = self.kw.get('join') or {}
        t = rel = jk = rb2 = None

        if on is not None:
            na == 1 or die.n_args('join(on=...)', 1, 'p', na)
            if kw:
                t = args[0]
                t == self.as_ or t in t2j or die.no_ta(t)
                jk, m = next(iter(kw.items()))
            else:
                m = args[0]
            isinstance(m, ModelClass) or die.type(m, ModelClass)
        else:
            if kw:
                na < 2 or die.n_args('join(rel=...)', '0 or 1', 'p', na)
                t = args[0] if args else None
                jk, rel = next(iter(kw.items()))
                isinstance(rel, Relation) or die.type(rel, Relation, kw=jk)
                t = self._join_rel(t, rel.name, rel.model)[0]
            else:
                t, rel = args if na == 2 else (None, args[0]) if na == 1 else \
                    die.n_args('join()', '1 or 2', 'p', na)
                if isinstance(rel, Relation):
                    t = self._join_rel(t, rel.name, rel.model)[0]
                else:
                    t, rel = self._join_rel(t, rel)
                jk = rel.join_key

            def on(as_):
                return rel.query(t, as_).where_sql(verbose=True, wrap=False)
            m = rel.rel_model
            rb2 = rel.reverse_b2

        if as_ is None:
            as_ = self.next_t(m)
        elif as_ == self.as_ or as_ in t2j:
            raise TypeError('not unique table alias: %r' % as_)
        else:
            as_ or die.no_ta(m)
            self._assert_ta(as_)

        if callable(on):
            on = on(as_)
        isinstance(on, SQL) or die.type(on, SQL, kw='on')

        return self._clone({
            **self.kw, 'join': {**t2j, as_: (prefix, m, on, t, jk, rb2)}})

    def last_t(self):
        t = self.as_
        for t in (self.kw.get('join') or ()):
            pass
        return t

    def left_join(self, *args, **kw):
        kw['prefix'] = 'LEFT JOIN'
        return self.join(*args, **kw)

    limit = _clone_setter('limit', int)

    def next_t(self, m=None):
        as_ = self.as_
        ts = self.kw.get('join') or ()
        if m is not None:
            t = m.DB_TABLE_AS
            if t != as_ and t not in ts:
                return t
        for i in range(len(ts) + 2, 0, -1):
            t = 't' + str(i)
            if t != as_ and t not in ts:
                return t

    offset = _clone_setter('offset', int)

    def or_where(self, *args, **kw):
        return self.where(*args, _op_='OR', **kw)

    def order_by(self, *args):
        if args == (None,):
            return self._clone({**self.kw}, 'order_by')

        match = self.RE_ORDER_BY.match
        tms = self._iter_tms
        xs = []
        for x in (args or die.n_args('order_by()', 'some', 'p', 0)):
            if isinstance(x, str):
                t, v, k = (match(x) or die.bad_col(x)).groups()
                ts = tuple(t for t, m in tms(t) if k in m.COLUMNS)
                ts or die.bad_col(k)
                len(ts) == 1 or die.amb_col(k)
                xs.append((ts[0], v))
            elif isinstance(x, SQL):
                xs.append(x)
            else:
                die.bad_col(x)

        return self._clone({**self.kw, 'order_by': tuple(xs)})

    def peek(self):
        return next(self._iter_rows(True), None)

    def pluck(self, cols):
        return self.model.DB.pluck(*self.select(SQL(cols)).sql())

    def select(self, *args, **kw):
        args or kw or die.n_args('select()', 'some', 'p/k', 0)
        return self._select({}, args, kw)

    def sql(self):
        return self._build(self.kw.get('type') or 'SELECT')

    @property
    def total_count(self):
        c = getattr(self, '_total_count', None)
        if c is None:
            c = self._total_count = self.count()
        return c

    @total_count.setter
    def total_count(self, v):
        self._total_count = v

    def update(self, *args, **kw):
        kw or die.n_args('update()', 'some', 'k', 0)
        if args:
            len(args) == 1 or die.n_args('update()', 1, 'p', len(args))
            t = args[0]
            cs = next(self._iter_tms(t))[1].COLUMNS
        else:
            t = self.as_
            cs = self.model.COLUMNS
        xs = tuple(
            (t, k + (' = ' if isinstance(x, SQL) else ' = %s'), x)
            for k, x in kw.items() if k in cs or die.bad_col(k))
        return self._clone({**self.kw, 'type': 'UPDATE', 'set': xs})

    def where(self, *args, _op_='AND', **kw):
        tms = None
        if args and isinstance(args[0], str):
            tms = tuple(self._iter_tms(args[0]))
            args = args[1:]

        xs = []
        for x in args:
            isinstance(x, SQL) or die.type(x, SQL)
            if x:
                xs and xs.append('AND')
                xs.append(x)

        for k, v in kw.items():
            tms = tms or tuple(self._iter_tms())
            ts = tuple(t for t, m in tms if k in m.COLUMNS)
            ts or die.bad_col(k)
            len(ts) == 1 or die.amb_col(k)

            xs and xs.append('AND')
            xs.append(
                (ts[0], k + ' IS NULL') if v is None else
                (ts[0], k + ' = %s', v) if not isinstance(v, SQL) else
                (ts[0], k + ' ', v) if isinstance(v, Operator) else
                (ts[0], k + ' = ', v))
        if not xs:
            return self

        wss = self.kw.get('where')
        wss = ((), tuple(xs)) if not wss else \
            (wss[0], (*wss[1], _op_, *xs)) if len(wss[1]) < 20 else \
            ((*wss[0], wss[1]), (_op_, *xs))
        return self._clone({**self.kw, 'where': wss})

    def where_sql(self, *, verbose=None, wrap=True):
        sql = self._build('WHERE_', wp=verbose)
        if sql:
            _s, _vs = sql._t
            sql._t = (f'({_s[6:]})' if wrap else _s[6:], _vs)
        return sql

    def with_select(self, *args, **kw):
        args or kw or die.n_args('with_select()', 'some', 'p/k', 0)
        t2xs = self.kw.get('select')
        return self._select({**t2xs}, args, kw) if t2xs else \
            self._select({}, (*self.model.DB_SELECT, *args), kw)
