from __future__ import print_function


class BaseConnector(object):

    def get_reader(self):
        return NotImplementedError()

    def get_writer(self):
        return NotImplementedError()
