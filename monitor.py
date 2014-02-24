# coding:utf-8
__author__ = 'lilx at 14-2-13'
import time
import os
import gevent
import json
from datetime import datetime


class Monitor(object):
    """
    log file monitor
    """
    def __init__(self, cfg):
        """
        with_history : bool if check the history file
        """
        self.cfg = cfg
        self.with_history = self.cfg.get('with_history')
        self.file_path = self.cfg['file_path']
        self.progress_path = ''
        self.progress = {}
        self.handlers = []
        self.listeners = {}
        self.running = False
        self.changes = 0

    def register_handler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def handle(self, content):
        for handler in self.handlers:
            handler(content)

    def report_progress(self, listener):
        pre_position = self.progress[listener.file_path].get('position', 0)
        cur_position = listener.progress.get('position', 0)
        cur_timestamp = listener.progress.get('timestamp', 0)
        self.progress[listener.file_path]['timestamp'] = cur_timestamp
        self.progress[listener.file_path]['position'] = cur_position
        if pre_position != cur_position:
            self.changes += 1
            if self.changes >= self.cfg['change_to_save']:
                self.save_progress()
                self.changes = 0
        elif listener.file_path != self.file_path:
            listener.stop()

    def start(self):
        self.load_progress()
        self.bind_listener()
        self.running = True
        for listener in self.listeners.values():
            listener.start()

    def stop(self):
        for listener in self.listeners.values():
            listener.stop()
        self.save_progress()

    def load_progress(self):
        p = os.path.join('data', 'progress_monitor_' + self.cfg['name'])
        self.progress_path = p
        if os.path.isfile(p):
            f = open(p, 'rb')
            self.progress = json.load(f)
        if self.file_path not in self.progress:
            self.progress[self.file_path] = {
                'position': 0
            }

    def save_progress(self):
        p = os.path.join('data', 'progress_monitor_' + self.cfg['name'])
        json.dump(self.progress, open(p, 'wb'))

    def bind_listener(self):
        main_progress = self.progress[self.file_path]
        timestamp = main_progress.get('timestamp')
        if timestamp and self.with_history:
            pt = datetime.fromtimestamp(timestamp)
            history_log_path = self.file_path + '.' + pt.strftime(self.cfg['suffix_date_format'])
            if history_log_path not in self.progress:
                self.progress[history_log_path] = {
                    'position': 0
                }
        for file_path, progress in self.progress.iteritems():
            self.listeners[file_path] = Listener(self, file_path, progress, {})


class Listener(object):
    """
    日志监听器
    """
    def __init__(self, monitor, file_path, progress, cfg):
        self.monitor = monitor
        self.file_path = file_path
        self.progress = progress
        self.cfg = cfg
        self.running = False
        self._file = None
        self.file_id = None

    def start(self):
        if not self.running:
            self.running = True
            gevent.spawn(self.__listen)

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