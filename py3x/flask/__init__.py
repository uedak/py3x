from base64 import urlsafe_b64encode
from flask import abort, current_app, redirect, render_template, \
    request as req, session, url_for
from flask.sessions import SecureCookieSession, SessionInterface
from jinja2.exceptions import TemplateNotFound
from pprint import pformat
from py3x.utils import AsJsonEncoder, Util, cached_property, die, try_, warn
from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import BadRequestKeyError, InternalServerError
import flask
import jinja2
import logging
import os
import time


class JSONEncoder(AsJsonEncoder, flask.app.json.JSONEncoder):
    pass


class SidSession(SecureCookieSession):
    def __init__(self, salt, sid):
        super().__init__()
        self.salt = salt
        d = sid and len(sid) == 30 and self.digest(sid[:22].encode())
        self.sid0 = sid if d and d == sid[-8:].encode() else None
        self.sid = self.sid0 or self.gen_sid()

    def csrf_token(self):
        return self.digest(self.sid.encode()).decode()

    def digest(self, v):
        from hashlib import md5
        return urlsafe_b64encode(md5(v + self.salt).digest())[:8]

    def gen_sid(self):
        from uuid import uuid4
        k = urlsafe_b64encode(uuid4().bytes)[:22]
        return (k + self.digest(k)).decode()

    def reset(self):
        self.clear()
        self.sid = self.gen_sid()


class RedisSessionInterfaceWithQpm(SessionInterface):
    import pickle as serializer
    session_class = SidSession

    def __init__(self, redis, prefix, *, expire=1800, max_qpm=50, blocks=300):
        self.redis = redis
        self.prefix = prefix
        self.expire = expire
        self.max_qpm = max_qpm
        self.blocks = blocks

        sk = prefix + 's'
        self.salt0 = redis.get(sk)
        if not self.salt0:
            import random
            self.salt0 = os.urandom(random.randint(128, 256))
            if not redis.set(sk, self.salt0, nx=True):
                self.salt0 = redis.get(sk) or die(sk)

    def _exec_pl(self, ses, p, max_qpm, fn):
        xk = f'{self.prefix}{p}:{ses.qpm_id}'
        xmk = ses.qpm_key = f'{self.prefix}{p}{ses.qpm_mm}:{ses.qpm_id}'
        pl = self.redis.pipeline()
        pl.get(xk)
        pl.incr(xmk)
        pl.expire(xmk, 60)
        fn and fn(pl)
        rs = pl.execute()
        rs[0] and abort(429)
        if max_qpm and (rs[1] or 0) >= max_qpm:
            if self.redis.set(xk, 1, ex=self.blocks, nx=True):
                warn(f'429 Too Many Requests: {req.url}')
            abort(429)
        return rs

    def _set_cookie(self, res, app, sid):
        res.set_cookie(app.session_cookie_name, sid, path='/', secure=True)

    def limit_qpm_by(self, uid, ses, max_qpm=None):
        if max_qpm is None:
            max_qpm = self.max_qpm
        k = ses.qpm_key
        ses.qpm_id = uid
        self._exec_pl(ses, 'u', max_qpm, lambda pl: pl.decr(k))

    def load_session(self, ses):
        k = ses.dk0
        rs = self._exec_pl(ses, 'i', self.max_qpm, k and (
            lambda pl: (pl.get(k), pl.expire(k, self.expire))))
        val = k and rs[3] and self.serializer.loads(rs[3])
        if val:
            ses.update(val)
            ses.modified = False
        else:
            ses.dk0 = None

    def open_session(self, app, req):
        ses = self.session_class(
            self.salt0 + req.environ.get('HTTP_HOST', '?').encode(),
            req.cookies.get(app.session_cookie_name))
        ses.qpm_mm = '%02d' % (time.time() // 60 % 60)
        ses.qpm_id = req.remote_addr
        ses.dk0 = ses.sid0 and f'{self.prefix}d:{ses.sid0}'
        return ses

    def save_session(self, app, ses, res):
        getattr(ses, 'accessed', None) and res.vary.add('Cookie')

        if ses.sid != ses.sid0:
            self._set_cookie(res, app, ses.sid)
            ses.dk0 and self.redis.delete(ses.dk0)
            ses.dk0 = None

        if not ses.modified:
            pass
        elif not ses:
            ses.dk0 and self.redis.delete(ses.dk0)
        else:
            dk = f'{self.prefix}d:{ses.sid}'
            v = self.serializer.dumps(dict(ses))
            if dk == ses.dk0:
                self.redis.setex(dk, self.expire, v)
            else:
                self.redis.set(dk, v, ex=self.expire, nx=True) or die(dk)


class Request(flask.Request):
    ABORT_ON_BAD_CSRF = True
    CSRF_TOKEN_NAME = '_csrf_token'
    from ..utils import Util
    _prepare_form = None

    @classmethod
    def dump_params(cls, d):
        d = try_(lambda: {k: v for k, v in d.lists()}, d)  # MultiDict?
        f = pformat(d, indent=4)
        return "{\n %s,\n}" % f[1:-1] if "\n" in f else f

    @cached_property
    def csrf_token(self):
        if hasattr(session, 'csrf_token'):
            return session.csrf_token()
        raise NotImplementedError

    def dump(self):
        return "%s\n%s %s %s\n%s%s" % (
            self.remote_addr, self.method, self.path,
            self.dump_params(getattr(self, '_args', self.args)),
            str(self.headers).replace("\r\n", "\n")[:-1],
            self.dump_params(req.json or getattr(self, '_form', self.form)))

    def is_valid_post(self, method='POST'):
        if self.method == method:
            if self.form.get(self.CSRF_TOKEN_NAME) == self.csrf_token:
                if not self._prepare_form:
                    self.prepare_form()
                    self._prepare_form = True
                return True
            self.ABORT_ON_BAD_CSRF and abort(400)

    def merged_args(self, *args, **kw):
        len(args) < 2 or die.n_args('merged_args()', '0 or 1', 'p', len(args))
        q = MultiDict(args[0] if args else self.args)
        for k, v in kw.items():
            if v is not None:
                q[k] = v
            elif k in q:
                del q[k]
        return q

    def prepare_form(self):
        f = self.form
        s = Util.RE_CRLF_OR_CR.sub
        for k in f:
            vs = dict.__getitem__(f, k)
            for i, v in enumerate(vs):
                if "\r" in v:
                    vs[i] = s("\n", v)

    def redirect_for(self, to, **kw):
        return redirect(self.url_for(to, **kw))

    @cached_property
    def uri(self):
        g = self.environ.get
        return g('RAW_URI') or g('REQUEST_URI') or \
            (self.full_path if self.query_string else self.path)

    def url_for(self, to, **kw):
        if to[0] == '.':
            to = self.blueprint + to
        vs = self.view_args
        rs = vs and current_app.url_map._rules_by_endpoint.get(to)
        if rs:
            for k in rs[0].arguments:
                if k not in kw and k in vs:
                    kw[k] = vs[k]
        return url_for(to, **kw)


class Response(flask.Response):
    autocorrect_location_header = False  # RFC7321

    def no_cache_header(self):
        h = self.headers
        h['Cache-Control'] = 'no-cache, no-store, must-revalidate, private'
        h['Expires'] = '0'
        h['Pragma'] = 'no-cache'
        return self


class WebApp(flask.Flask):
    json_encoder = JSONEncoder
    request_class = Request
    response_class = Response

    @classmethod
    def import_controllers(cls, dir, pfx):
        from importlib.machinery import SourceFileLoader as ldr
        for cwd, ds, fs in os.walk(dir):
            base = pfx + cwd[len(dir):].replace('/', '.')
            for f in sorted(fs):
                if f.endswith('.py'):
                    pkg = '.'.join((base, f[:-3]))
                    path = '/'.join((cwd, f))
                    yield ldr(pkg, path).load_module()
            ds[:] = sorted([d for d in ds if d != '__pycache__'])

    def handle_500(self, e):
        res = None
        try:
            res = self.handle_http_error(500)
        except Exception as e2:
            warn(e2)
        return res or InternalServerError()

    def handle_http_error(self, cd):
        if self.has_error_template(cd):
            return render_template(f'/{cd}.jinja2'), cd

    def handle_http_exception(self, e):  # @Override
        if isinstance(e, BadRequestKeyError):
            warn(e)
        elif self._debug and e.code == 404:
            warn(e, level=logging.DEBUG)
        return self.handle_http_error(e.code) or \
            super().handle_http_exception(e)

    def has_error_template(self, cd):
        h = self._has_error_template.get(cd)
        if h is None:
            h = self._has_error_template[cd] = False
            try:
                self.jinja_env.get_or_select_template(f'/{cd}.jinja2')
                h = self._has_error_template[cd] = True
            except TemplateNotFound:
                pass
        return h

    def log_exception(self, exc_info):  # @Override
        self.logger.error('', exc_info=exc_info)

    def setup(self, view_roots, debug=False):
        self._debug = debug
        self._has_error_template = {}
        self.config['JSON_AS_ASCII'] = False
        self.config['PROPAGATE_EXCEPTIONS'] = False
        self.logger.handlers.clear()
        self.logger.propagate = True

        je = self.jinja_env
        je.auto_reload = debug
        je.autoescape = True
        je.finalize = lambda v: '' if v is None else v
        je.globals.update(req=req)
        je.join_path = lambda path, cur: die(path) if '..' in path else \
            path if path[0] == '/' else \
            (cur[:cur.rfind('/') + 1] + path)
        je.loader = jinja2.FileSystemLoader((
            *view_roots, os.path.abspath(__file__ + '/../views')))

        @self.before_request
        def before_request():
            req._args = req.args
            req._form = req.form
            req.args = MultiDict(req.args)
            req.form = MultiDict(req.form)
            debug and print("\n%s\n%s %s %s\n%s\n" % (
                '=' * Util.TERM_W, req.method, req.path,
                req.dump_params(req._args),
                req.dump_params(req.json or req._form)))

        @self.errorhandler(500)
        def handle_500(e):
            return self.handle_500(e)


class WebAppCtrl(flask.Blueprint):
    def redirect_for(self, to, **kw):
        return redirect(url_for(to, **kw))

    def render(self, **kw):
        return self.render_as(req.endpoint, **kw)

    def render_as(self, ep, **kw):
        if ep.startswith('.'):
            ep = req.blueprint + ep
        elif ep.startswith('root.'):
            ep = ep[5:]
        return render_template(f"/{ep.replace('.', '/')}.jinja2", **kw)
