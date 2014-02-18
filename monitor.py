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

    def read(self):
        c = []
        for i in xrange(self.lines):
            l = self.file.readline()
            if not l:
                break
            c.append(l)
        if c:
            self.handle(c)

    def start(self):
        if not self.running:
            self.running = True
            self.file_id = self.get_file_id(os.stat(self.file_path))
            self.file = open(self.file_path, 'rb')
            self.__monitor()

    def __monitor(self):
        while self.running:
            gevent.sleep(self.interval)
            file_id = self.get_file_id(os.stat(self.file_path))
            if file_id != self.file_id:
                self.file = open(self.file_path, 'rb')
                self.position = 0
                self.timestamp = time.time()
            self.read()
            self.position = self.file.tell()
            self.timestamp = time.time()

    @staticmethod
    def get_file_id(st):
        if 'posix' == os.name:
            return '%x_%x' % (st.st_dev, st.st_ino)
        else:
            return '%f' % st.st_ctime