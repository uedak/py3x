from . import RecordNotFound, SQL, die
from ..utils import _NX, cached_class_property, cached_property, repr_kw


def _attr_in_db(self, obj, cls=None, col=None, nld=die):
    if obj is None:
        return self.col

    d = obj.__dict__
    if '.k2i' in d:
        k2i = d['.k2i']
        if col is None:
            col = self.col
        k = col.name
        if k not in k2i:
            nld is die and die.col_nld(k)
            return nld
        v = d['.dbvs'][k2i[k]]
        db2py = col.db2py
        if db2py:
            if db2py is not True:
                v = db2py(v)
            if k not in d:
                d[k] = v
        return v


class ModelClass(type):
    # def __new__(cls, *args):
    #     cls = super().__new__(cls, *args)
    #     cls.__init_subclass__()
    #     return cls

    def __repr__(self):
        return self.__name__


class Model(ModelClass('ModelClass', (), {})):
    from . import BulkLoader
    from ..errors import Errors

    class NO_CHANGES:
        pass

    DB_INDEXES = ()
    MAX_PER_PAGE = 1000
    PER_PAGE = 25
    PRIMARY_KEY = ()

    @classmethod
    def __init_subclass__(cls):
        if 'DB' in cls.__dict__:
            cls.MODEL_BASE = cls
            cls.MODELS = {}
        if 'DB_TABLE' in cls.__dict__:
            setattr(cls.MODEL_BASE, cls.__name__, cls)
            cls.MODELS[cls.__name__] = cls
            cls.after_def()

    @classmethod
    def _instantiate_(cls, k2i, vs, fck, fc):
        r = object.__new__(cls)
        d = r.__dict__
        d['.k2i'] = k2i
        d['.dbvs'] = vs
        if fck and fck not in fc:
            fc[fck] = r
            d['.fck'] = fck
        return r

    @cached_class_property
    def AUTO_INCREMENT(cls):
        pk = cls.PRIMARY_KEY
        pk = len(pk) == 1 and cls.COLUMNS[pk[0]]
        return pk and pk.auto_increment and pk.name or None

    @cached_class_property
    def COLUMN_DEFAULTS(cls):
        return {k: c.default for k, c in cls.COLUMNS.items()
                if c.default is not None}

    @cached_class_property
    def CREATED_BY(cls):
        return 'created_by' if 'created_by' in cls.COLUMNS else None

    @cached_class_property
    def DB_SELECT(cls):
        return ('*',)

    @cached_class_property
    def DB_TABLE_AS(cls):
        return 't1'

    @cached_class_property
    def INIT_BY(cls):
        ks = (cls.CREATED_BY, cls.UPDATED_BY)
        return tuple((k, cls.COLUMNS[k].as_) for k in ks if k)

    @cached_class_property
    def LOCK_VERSION(cls):
        return 'lock_version' if 'lock_version' in cls.COLUMNS else None

    @cached_class_property
    def Query(cls):
        return cls.DB.Query

    @cached_class_property
    def RESTORE_AFTER_UPDATE(cls):
        ks = (cls.LOCK_VERSION, cls.UPDATED_AT, cls.UPDATED_BY)
        return tuple(k for k in ks if k)

    @cached_class_property
    def SHARED_INDEX(cls):
        return cls.DB.shared_index(tuple(cls.COLUMNS))

    @cached_class_property
    def UNIQUE_COLUMNS(cls):
        ks = {k for ks in cls.VALIDATE_UNIQUENESS for k in ks}
        return {k: c for k, c in cls.COLUMNS.items() if k in ks}

    @cached_class_property
    def UPDATED_AT(cls):
        return 'updated_at' if 'updated_at' in cls.COLUMNS else None

    @cached_class_property
    def UPDATED_BY(cls):
        return 'updated_by' if 'updated_by' in cls.COLUMNS else None

    @cached_class_property
    def Util(cls):
        return cls.DB.Util

    @cached_class_property
    def VALIDATE_UNIQUENESS(cls):
        return (
            *(i['on'] for i in cls.DB_INDEXES if i.get('prefix') == 'UNIQUE'),
            *((k,) for k, c in cls.COLUMNS.items() if c.unique))

    @classmethod
    def after_def(cls):
        from .columns import Column
        from .relations import Relation

        attrs = cls.ATTRS = {}
        cls.BELONGINGS = {}
        cs = cls.COLUMNS = {}
        ras = cls.RESERVED_ATTRS
        for k, v in cls.__dict__.items():
            if isinstance(v, Column):
                attrs[k] = cs[k] = v
            elif isinstance(v, Relation):
                attrs[k] = v
            else:
                continue  # pragma: no cover
            k in ras and die(TypeError(f"{cls}: attr name '{k}' is reserved"))

        pk = cls.PRIMARY_KEY = cls.PRIMARY_KEY or \
            tuple(k for k, c in cs.items() if c.primary_key)
        for k, v in tuple(attrs.items()):
            v.after_def(cls, k)
        if pk:
            s = cls.DB_SELECT
            k = 'COLUMNS' if (*cs,)[:len(pk)] != pk else \
                'DB_SELECT' if s[0] != '*' and s[:len(pk)] != pk else None
            k and die(TypeError(f'{cls}.{k} does not start with PRIMARY_KEY'))
            all(die.incompo(cs[k], 'primary_key', 'db2py')
                for k in pk if cs[k].db2py not in (None, True))

    @classmethod
    def as_(cls, as_):
        return cls.query(as_)

    @classmethod
    def belongs_to_x_as(cls, x, k):
        return k[:-3] if k.endswith('_id') else \
            'creator' if k == cls.CREATED_BY else \
            'updater' if k == cls.UPDATED_BY else None

    @classmethod
    def bulk_loader(cls, columns=None, **kw):
        return cls.BulkLoader(
            cls.DB, cls.DB_TABLE, columns or tuple(cls.COLUMNS), **kw)

    @classmethod
    def create_index_sqls(cls):
        t = cls.DB_TABLE
        return ['CREATE%sINDEX %s ON %s (%s)' % (
            (f' {p} ' if p else ' '), '_'.join((t, *on)), t, ', '.join(on)
        ) for p, on in ((i.get('prefix'), i['on']) for i in cls.db_indexes())]

    @classmethod
    def create_table_sql(cls):
        pk = cls.PRIMARY_KEY
        cs = cls.COLUMNS.values()
        return "CREATE TABLE %s (\n  %s\n)" % (cls.DB_TABLE, ",\n  ".join((
            *(' '.join(c.ddl_sqls()) for c in cs),
            *(('PRIMARY KEY (%s)' % ', '.join(pk),) if len(pk) > 1 else ()),
            *('FOREIGN KEY (%s) REFERENCES %s (%s)' % (
                c.name, c.belongs_to.DB_TABLE, c.belongs_to.PRIMARY_KEY[0])
                for c in cs if c.belongs_to and c.foreign_key)
        )))

    @classmethod
    def db_indexes(cls):
        xs = [*cls.DB_INDEXES]
        c0s = {cs[0] for cs in (cls.PRIMARY_KEY, *(i['on'] for i in xs)) if cs}
        for k, c in cls.COLUMNS.items():
            i = False if c.index is False else \
                dict(prefix='UNIQUE', on=(k,)) if c.unique else \
                dict(on=(k,)) if c.index and k not in c0s else None
            i and (xs.append(i), c0s.add(k))
        return xs

    @classmethod
    def ddl_sqls(cls):
        return [cls.create_table_sql(), *cls.create_index_sqls()]

    @classmethod
    def find(cls, *args):
        pk = cls.PRIMARY_KEY or die.no_pk(cls)
        len(args) == len(pk) or die.n_args(
            f'{cls}.find()', len(pk), 'p', len(args))
        any(isinstance(v, SQL) and die(TypeError(v)) for v in args)
        r = cls.find_by(**dict(zip(pk, args)))
        if r is not None:
            return r
        raise RecordNotFound(cls.DB_TABLE, *args)

    @classmethod
    def find_by(cls, **kw):
        kw or die.n_args('find_by()', 'some', 'k', 0)
        pk = cls.PRIMARY_KEY
        wpk = pk and len(kw) == len(pk) and all(k in kw for k in pk)
        if wpk:
            for k, v in kw.items():
                if v is None:
                    return
                c = cls.COLUMNS[k]
                pt = c.PY_TYPE
                if not isinstance(v, pt):
                    if isinstance(v, SQL):
                        wpk = False
                        break
                    v = kw[k] = c.form2py(v)
                    if not isinstance(v, pt):
                        return
        fc = wpk and cls.DB.find_cache
        r = fc.get((cls.DB_TABLE, *(kw[k] for k in pk))) if fc else None
        return r if r is not None else \
            cls.query().where(**kw).peek() if wpk else \
            cls.query().where(**kw).first()

    instantiate = _instantiate_

    @classmethod
    def query(cls, as_=None):
        if as_ is None:
            as_ = cls.DB_TABLE_AS
        elif as_ != '':
            cls.Query._assert_ta(as_)
        return cls.Query(cls, as_)

    @classmethod
    def where(cls, *args, **kw):
        q = cls.query()
        return q.where(*args, **kw) if args or kw else q

    def __contains__(self, k):
        d = self.__dict__
        return k in d or '.k2i' in d and k in d['.k2i'] or \
            '_items_' in d and k in d['_items_']

    def __getitem__(self, k):
        v = getattr(self, k) if k in self.ATTRS else self.get(k, _NX)
        return v if v is not _NX else die(KeyError(k))

    def __init__(self, **kw):
        attrs = self.ATTRS
        for k, v in kw.items():
            if k == 'by':
                m = isinstance(v, Model)
                for k1, k2 in self.INIT_BY:
                    setattr(self, k2 if m else k1, v)
            elif k in attrs:
                setattr(self, k, v)
            else:
                self[k] = v
        for k, v in self.COLUMN_DEFAULTS.items():
            k in kw or setattr(self, k, v(self) if callable(v) else v)

    def __repr__(self, verbose: bool = False):
        if verbose:
            r = repr_kw(self.dict(True))
        else:
            npk = len(self.PRIMARY_KEY)
            rs = []
            for i, k in enumerate(self.COLUMNS):
                if k in self:
                    if i < npk:
                        rs.append('%s=%r' % (k, getattr(self, k)))
                    else:
                        rs.append('...')
                        break
            r = ', '.join(rs)
        return f"{self.__class__.__name__}({r})"

    def __setitem__(self, k, v):
        a = self.ATTRS.get(k)
        if a:
            form2py = a.form2py
            setattr(self, k, form2py(v) if form2py else v)
        else:
            self._items_[k] = v

    def _after_delete_(self, d, kw):
        if '.fck' in d:
            fck = d.pop('.fck')
            fc = self.DB.find_cache
            fc and fc.pop(fck, None)
        return 1

    def _after_insert_(self, d, txn, kw):
        k2i = d['.k2i'] = self.SHARED_INDEX
        dbvs = d['.dbvs'] = [None] * len(k2i)
        for k, v in txn.items():
            dbvs[k2i[k]] = v

        k = self.LOCK_VERSION
        if k and k in d:
            del d[k]
        return 1

    def _after_update_(self, d, txn, kw):
        k2i = d['.k2i']
        dbvs = d['.dbvs']
        if any(k not in k2i for k in txn):
            l0 = len(k2i)
            k2i = d['.k2i'] = self.DB.shared_index(tuple({**k2i, **txn}))
            dbvs = d['.dbvs'] = [*dbvs, *((None,) * (len(k2i) - l0))]
        elif type(dbvs) is tuple:
            dbvs = d['.dbvs'] = [*dbvs]
        for k, v in txn.items():
            dbvs[k2i[k]] = v

        ks = self.RESTORE_AFTER_UPDATE
        ks = ks and tuple(k for k in ks if k in txn and k in d)
        ks and self.restore_attrs(*ks)

        pk = '.fck' in d and self.PRIMARY_KEY
        if pk and any(k in txn for k in pk):
            fck = d.pop('.fck')
            fc = self.DB.find_cache
            if fc and fc.pop(fck, None) is self:
                fck = d['.fck'] = (fck[0], *(dbvs[k2i[k]] for k in pk))
                fc[fck] = self
        return 1

    @cached_property
    def _items_(self):
        return {}

    def _update_txn_(self, d, txn, w, kw):
        upd = None

        lock = kw.get('lock')
        k = lock is not False and self.LOCK_VERSION
        if k:
            if k in txn:  # CONFLICT
                return
            if k in d:
                v = w[k] = d[k]
                txn[k] = (v + 1) % 100
            elif lock:
                upd = {k: SQL(f'({k} + 1) %% 100')}

        ts = kw.get('timestamp')
        k = ts is not False and self.UPDATED_AT
        if k and k not in txn:
            v = NOW(self)
            if ts or v != _attr_in_db(None, self, None, self.COLUMNS[k], None):
                txn[k] = v

        by = kw.get('by')
        k = by is not None and self.UPDATED_BY
        if k and k not in txn:
            c = self.COLUMNS[k]
            v = getattr(by, c.b2pk) if isinstance(by, c.belongs_to) else by
            isinstance(v, c.PY_TYPE) or die.type(
                by, c.PY_TYPE, c.belongs_to, kw='by')
            if v != _attr_in_db(None, self, None, c, None):
                txn[k] = v

        return {**txn, **upd} if upd else txn

    def attr_in_db(self, k):
        return _attr_in_db(None, self, None, self.COLUMNS[k])

    def delete(self, **kw):
        d = self.__dict__
        n = self.where(**self.pk_dict_in_db(d)).delete().execute()
        return n and self._after_delete_(d, kw)

    def dict(self, full=False):
        d = self.__dict__
        k2i = d['.k2i'] if '.k2i' in d else ()
        r = {k: getattr(self, k) for k in self.COLUMNS if k in d or k in k2i}
        if full:
            for ks in (d['_items_'] if '_items_' in d else (), k2i):
                for k in ks:
                    if k not in r:
                        r[k] = self[k]
        return r

    @cached_property
    def errors(self):
        return self.Errors()

    def get(self, k, nx=None):
        if k in self.ATTRS:
            return getattr(self, k)
        d = self.__dict__
        if '_items_' in d:
            its = d['_items_']
            if k in its:
                return its[k]
        if '.k2i' in d:
            k2i = d['.k2i']
            if k in k2i:
                return d['.dbvs'][k2i[k]]  # for sub query
        return nx

    def getlist(self, k):  # as form (MultiDict)
        vs = self.get(k, None)
        if vs is None:
            return []
        s = self.Util.val2str
        return [s(v) for v in vs] if isinstance(vs, (list, set, tuple)) else \
            [s(vs)]

    def insert(self, **kw):
        txn = self.is_changed(txn={})
        sql = (f"INSERT INTO {self.DB_TABLE} ({', '.join(txn)}) "
               f"VALUES ({', '.join(('%s',) * len(txn))})")
        ai = self.AUTO_INCREMENT
        if ai and ai not in txn:
            txn[ai] = self.DB.execute_insert(sql, tuple(txn.values()), ai)
        else:
            self.DB.execute(sql, tuple(txn.values()))
        return self._after_insert_(self.__dict__, txn, kw)

    def is_changed(self, *ks, txn=False):
        d = self.__dict__
        k2i = d['.k2i'] if '.k2i' in d else ()
        dbvs = k2i and d['.dbvs']
        cs = self.COLUMNS
        for k, c in (((k, cs[k]) for k in ks) if ks else cs.items()):
            if k in d:
                v = d[k]
            else:
                v = c.txn_value
                if v is None:
                    continue
                v = v(d)
                if v is _NX:
                    continue

            dbv = dbvs[k2i[k]] if k in k2i else _NX
            if dbv is _NX and txn is False:
                return True

            py2db = c.py2db
            if py2db is not None:
                v = py2db(v)

            if dbv is _NX:
                txn[k] = v
            elif v != dbv:
                if txn is False:
                    return True
                txn[k] = v
        return txn

    def is_new_record(self):
        return '.k2i' not in self.__dict__

    is_uniqueness_changed = is_changed

    def is_valid(self, **kw):
        self.errors.clear()
        self.validate(**kw)
        return not self.errors

    def pk_dict_in_db(self, d=None):
        pk = self.PRIMARY_KEY or die.no_pk(self.__class__)
        '.k2i' in (d or self.__dict__) or die.col_nld(pk[0])
        return {k: _attr_in_db(None, self, None, self.COLUMNS[k]) for k in pk}

    def restore_attrs(self, *ks):
        d = self.__dict__
        is_new = '.k2i' not in d
        cs = self.COLUMNS
        for k, c in (((k, cs[k]) for k in ks) if ks else cs.items()):
            v = c.default if is_new else None
            if v is not None:
                setattr(self, k, v(self) if callable(v) else v)
            elif k in d:
                f = c.restore_attr
                if f is None:
                    del d[k]
                else:
                    f(d, k, self)
        return self

    def save(self, **kw):
        return self.insert(**kw) if self.is_new_record() else self.update(**kw)

    def set_items(self, **kw):
        for k, v in kw.items():
            self[k] = v
        return self

    def update(self, **kw):
        txn = self.is_changed(txn={})
        if not (txn or kw.get('force')):
            return self.NO_CHANGES

        d = self.__dict__
        w = self.pk_dict_in_db(d)
        upd = self._update_txn_(d, txn, w, kw)
        if upd is not None:
            if not upd:
                return self.NO_CHANGES
            if self.query().where(**w).update(**upd).execute():
                return self._after_update_(d, txn, kw)
        if upd is None or self.LOCK_VERSION in w:
            self.errors.add(None, self.errors.CONFLICT)
        return 0

    def validate(self, *, force=False, **kw):
        d = self.__dict__
        is_new = '.k2i' not in d
        es = self.errors
        k = not is_new and self.LOCK_VERSION
        if k and self.is_changed(k):
            es.add(None, es.CONFLICT)
            return

        for k, c in self.COLUMNS.items():
            v = c.validate
            if not v:
                pass
            elif k in d or force:
                self.validate_column(k)
            elif is_new and v.get('nn') and not c.auto_increment:
                nn = c.nn_on_create
                (nn is None or nn(d)) and self.validate_column(k)
        v = not es and kw.get('uniqueness', True) and self.VALIDATE_UNIQUENESS
        v and self.validate_uniqueness(*v)

    def validate_column(self, k, **kw):
        v = self.COLUMNS[k].validate
        v = {**v, **kw} if v and kw else (v or kw)
        return self.errors.validate(k, getattr(self, k), **v) if v else True

    def validate_item(self, k, **kw):
        return self.errors.validate(k, self[k], **kw) if kw else True

    def validate_uniqueness(self, *args):
        kss, ss, vs = [], [], []
        for ks in args:
            if self.is_uniqueness_changed(*ks):
                w = {k: getattr(self, k) for k in ks}
                if None not in w.values():
                    kss.append(ks)
                    _s, _vs = self.where(**w).exists_sql()
                    ss.append(_s)
                    _vs and vs.extend(_vs)

        if kss:
            xs = self.DB.execute('SELECT ' + ",\n".join(ss), vs, 1)
            for ks, x in zip(kss, xs):
                x and self.errors.add(ks[-1], self.errors.TAKEN)

    RESERVED_ATTRS = {*locals()}


def NOW(r):
    return r.Util.now()


def TODAY(r):
    return r.Util.today()
