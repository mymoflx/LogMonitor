# coding:utf-8
__author__ = 'lilx at 14-2-13'
import time
import os
import gevent
from datetime import datetime
from datetime import timedelta
from gevent import Greenlet


class Monitor(Greenlet):
    """
    log file monitor
    """
    def __init__(self, scheduler, config, progress):
        """
        cfg :
            file_path : str the monitor file path
            interval : int the rate to check the file
            lines : int line count read at a time
            position : int the read position of last time
            timestamp : float the last read timestamp
            with_history : bool if check the history file
            id : str the monitor log config id
        """
        Greenlet.__init__(self)
        self.scheduler = scheduler
        self.cfg = config
        self.progress = progress
        self.handlers = []
        self.running = False
        self.listeners = {}

    def register_handler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def handle(self, content):
        for handler in self.handlers:
            handler.handle(content)

    def report_progress(self, listener):
        file_pg = self.progress.get(listener.cfg['dk'], {})
        if file_pg.get('position') == listener.progress.get('position'):
            if '0' != listener.cfg['dk']:
                listener.stop()
                self.listeners.pop(listener.cfg['dk'])
            return None
        file_pg.update(listener.progress)
        self.progress[listener.cfg['dk']] = file_pg
        self.scheduler.save_progress(self)

    def _run(self):
        if not self.running:
            self.running = True
            self.__monitor()

    def stop(self):
        self.running = False
        for li, listener in self.listeners.iteritems():
            listener.stop()
        gevent.joinall(self.listeners.values())

    def __monitor(self):
        nt = datetime.now()
        ndk = nt.strftime('%Y-%m-%d')
        p0 = self.progress.get('0', {})
        if p0.get('timestamp', 0) > 0:
            # 计算指定日期
            dt = datetime.fromtimestamp(p0['timestamp'])
            da = timedelta(1)
            dk = dt.strftime('%Y-%m-%d')
            while ndk > dk:
                if dk not in self.progress:
                    self.progress[dk] = {
                        'timestamp': time.mktime(dt.timetuple()),
                        'position': 0
                    }
                dt += da
                dk = dt.strftime('%Y-%m-%d')
        for dk, pg in self.progress.items():
            if dk != '0':
                fp = self.cfg['file_path'] + '.' + dk
            else:
                fp = self.cfg['file_path']
            if not os.path.exists(fp):
                self.progress.pop(dk, None)
                continue
            cfg = {'dk': dk}
            cfg.update(self.cfg)
            progress = {}
            progress.update(pg)
            listener = Listener(self, fp, progress, cfg)
            listener.start()
            self.listeners[dk] = listener


class Listener(Greenlet):
    """
    log listener
    """
    def __init__(self, monitor, file_path, progress, cfg):
        Greenlet.__init__(self)
        self.monitor = monitor
        self.file_path = file_path
        self.progress = progress
        self.cfg = cfg
        self.running = False
        self._file = None
        self.file_id = None

    def _run(self):
        if not self.running:
            self.running = True
            self.__listen()

    def stop(self):
        self.running = False
        self._file.close()
        self._file = None

    def switch_file(self):
        st = os.stat(self.file_path)
        file_id = self.get_file_id(st)
        if self._file is None:
            self.file_id = file_id
            self._file = open(self.file_path, 'rb')
            self._file.seek(self.progress.get('position', 0))
        elif self.file_id != file_id and st.st_size <= self.progress.get('position', 0):
            self.file_id = file_id
            self._file = open(self.file_path, 'rb')
            self.progress['position'] = 0
            self.progress['timestamp'] = time.time()

    def __listen(self):
        while self.running:
            self.switch_file()
            lines = []
            for i in xrange(self.cfg['lines']):
                l = self._file.readline()
                if not l:
                    break
                lines.append(l)
            self.progress['position'] = self._file.tell()
            self.progress['timestamp'] = time.time()
            if lines:
                self.monitor.handle(lines)
            self.monitor.report_progress(self)
            gevent.sleep(self.cfg['interval'])

    @staticmethod
    def get_file_id(st):
        if 'posix' == os.name:
            return '%x_%x' % (st.st_dev, st.st_ino)
        else:
            return '%f' % st.st_ctime