# coding:utf-8
__author__ = 'lilx at 14-2-19'
import json
import gevent
from monitor import Monitor


class Scheduler(object):
    """
    日志监听调度器
    """
    def __init__(self):
        f = open('config/config.json', 'rb')
        self.cfg = json.load(f)
        g = open('data/progress.json', 'rb')
        self.progress = json.load(g)
        f.close()
        g.close()
        self.started = False
        self.monitors = {}
        self.changes = 0

    def start(self):
        dmg = self.cfg['default_monitor']
        for mg in self.cfg['monitor_logs']:
            base_path = mg.get('base_path', dmg['base_path'])
            fp = '/'.join([base_path, mg['file_name']])
            cfg = {'file_path': fp}
            cfg.update(dmg)
            cfg.update(mg)
            pg = self.progress.get(mg['id'], {})
            m = Monitor(self, cfg, pg)
            mc = mg['handler'].rsplit('.', 1)
            hm = __import__(mc[0], globals(), locals())
            hcls = getattr(hm, mc[1])
            handler = hcls()
            m.register_handler(handler)
            m.start()
            self.monitors[mg['id']] = m
        self.started = True

    def stop(self):
        self.started = False
        for mi, monitor in self.monitors.iteritems():
            monitor.stop()
        gevent.joinall(self.monitors.values())
        self.save_progress()

    def run(self):
        while self.started:
            gevent.sleep(10)

    def save_progress(self, monitor=None):
        if monitor:
            log_pg = self.progress.get(monitor.cfg['id'], {})
            log_pg.update(monitor.progress)
            self.progress[monitor.cfg['id']] = log_pg
        self.changes += 1
        if not monitor or self.changes >= self.cfg['change_to_save']:
            g = open('data/progress.json', 'wb')
            json.dump(self.progress, g, sort_keys=True, indent=4)
            g.close()
            self.changes = 0


sc = Scheduler()


# 系统发送term信号时保存缓存数据
import signal


def stop(*args):
    sc.stop()

gevent.signal(signal.SIGTERM, stop)

if __name__ == '__main__':
    sc.start()
    sc.run()