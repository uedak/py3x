class _Csr:
    def __init__(self, hs, *rs):
        self.description = tuple((h,) for h in hs)
        self.rs = rs

    def __iter__(self):
        return iter(self.rs)

    def fetchone(self):
        return next(iter(self.rs))


_X = [None]


def _x(hs, *rs):
    def x(sql, vs, as_):
        _X[0] = (sql, vs, as_)
        csr = _Csr(hs, *rs)
        return csr.fetchone() if as_ == 1 else csr
    return x


def instantiate(cls, **kw):
    return cls.instantiate(
        cls.DB.shared_index(tuple(kw)), tuple(kw.values()), None, None)


def last_x():
    return _X[0]


def r2csr(**r):
    return _x(r, tuple(r.values()))


def rs2csr(hs, *rs):
    return _x(hs, *rs)
