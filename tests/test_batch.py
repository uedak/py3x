from py3x.utils import Util
import py3x.batch
import logging
import os


def test_run_():
    class Bat(py3x.batch.Runner):
        def finalize_log(self, lgr):
            self.fin = super().finalize_log(lgr)

    class pkg:
        def run(args, lgr):
            pass

    dir = 'py3x.test'
    os.mkdir(dir)
    dlh = Util.init_logger(dir + '/bat.log', fmt=Bat.Formatter())
    lck = dir + '/bat.lck'

    bat = Bat(pkg, dlh, lck=lck)
    bat.run_([])
    assert bat.fin[0] == 20
    assert bat.fin[1] == 'END successfully'
    assert not os.path.exists(lck)

    os.mkdir(lck)
    bat = Bat(pkg, dlh, lck=lck)
    bat.run_([])
    assert bat.fin[0] == 40
    assert bat.fin[1] == 'FAILED'
    assert os.path.exists(lck)

    logging.captureWarnings(False)
    logging.getLogger().handlers.clear()
    Util.rm_rf(dir)
