from __future__ import print_function


class BaseProcessor(object):

    def __init__(self, conn):
        self.conn = conn

    def query(self, sql, *args, **kwargs):
        return NotImplementedError()

    def write(self):
        return NotImplementedError()
