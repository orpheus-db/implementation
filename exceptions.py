'''
Any system level general exceptions should be included here
Module related exception should go into each module
'''

class BadStateError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
