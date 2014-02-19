# coding:utf-8
__author__ = 'lilx at 14-2-13'
import time
import os
import gevent


class Monitor(object):
    """
    log file monitor
    """
    def __init__(self, file_path, interval, lines, position, timestamp, with_history):
        """
        file_path : str the monitor file path
        interval : int the rate to check the file
        lines : int line count read at a time
        position : int the read position of last time
        timestamp : float the last read timestamp
        with_history : bool if check the history file
        """
        self.file_path = file_path
        self.interval = interval
        self.lines = lines
        self.position = position
        self.timestamp = timestamp
        self.with_history = with_history
        self.handlers = []
        self.file_id = None
        self.file = None
        self.running = False

    def register_handler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def handle(self, content):
        for handler in self.handlers:
            handler(content)

    def report_progress(self, listener):
        pass

    def start(self):
        if not self.running:
            self.running = True
            self.__monitor()

    def __monitor(self):
        while self.running:
            pass


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