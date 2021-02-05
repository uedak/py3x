from .utils import die, warn
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
import logging
import os
import signal

PID = os.getpid()


class Runner:
    class Config:
        parallel_run = False
        summary_limit = 100

    class Formatter(logging.Formatter):
        N2C = {INFO: '+', WARNING: '?', ERROR: '!', CRITICAL: 'X'}

        @classmethod
        def grep(cls, lns):
            sps = f' {PID} '
            pos = 27 + len(sps)
            c2n = {c: n for n, c in cls.N2C.items()}
            for ln in lns:
                if ln and ln[0] == '[' and ln[27:pos] == sps:
                    yield c2n[ln[1]], ln

        def format(self, r):
            msg = super().format(r).strip().replace("\n", "\n\t")
            c = self.N2C.get(r.levelno)
            return ' - ' + msg if not c else (
                f'[{c}] {self.formatTime(r, self.datefmt)} {PID} {msg}')

    class SigTrap(Exception):
        pass

    def __init__(self, pkg, dlh, lck=None):
        self.pkg = pkg
        self.cfg = pkg.Config if hasattr(pkg, 'Config') else self.Config
        self.dlh = dlh
        self.lck = not self.cfg.parallel_run and lck

    def after_run(self):
        self.lck and os.rmdir(self.lck)

    def before_run(self):
        self.lck and os.mkdir(self.lck)

    def finalize_log(self, lgr):
        dlh = self.dlh
        lim = self.cfg.summary_limit
        lvn = INFO
        lns = []
        for n, ln in self.Formatter.grep(dlh.readlines()):
            if n > lvn:
                lvn = n
            if len(lns) <= lim:
                lns.append("...\n" if len(lns) == lim else ln)

        msg = self.lvn2msg(lvn)
        f0 = dlh.format

        def format(r):
            ln = f0(r)
            lns.append(ln + dlh.terminator)
            return ln

        dlh.format = format
        lgr._log(lvn, msg, ())
        dlh.stream.write(dlh.terminator)
        dlh.format = f0
        return lvn, msg, lns

    def lvn2msg(self, lvn):
        return 'END successfully' if lvn == INFO else \
            'END with warning' if lvn == WARNING else \
            'FAILED'

    def run(self, args):
        try:
            self.sigtrap('SIGINT')
            self.sigtrap('SIGTERM')
            self.run_(args)
        except Exception as e:
            warn(e, level=CRITICAL)

    def run_(self, args):
        lgr = logging.getLogger(self.pkg.__name__)
        lgr.setLevel(DEBUG)
        lgr.info(f'BEGIN {args}')
        try:
            self.before_run()
            try:
                self.pkg.run(args, lgr)
            except self.SigTrap as e:
                lgr.warn(f'caught {e}')
            except Exception as e:
                warn(e, level=ERROR)
            self.after_run()
        except Exception as e:
            warn(e, level=ERROR)
        self.finalize_log(lgr)

    def sigtrap(self, sig):
        signal.signal(getattr(signal, sig), lambda *a: die(self.SigTrap(sig)))
