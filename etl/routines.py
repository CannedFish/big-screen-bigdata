# -*- coding: utf-8 -*-

import abc
from threading import Thread, Condition
import time
import logging

LOG = logging.getLogger(__name__)

class MyThread(Thread):
    def __init__(self, name, tid):
        super(MyThread, self).__init__()
        self.threadID = tid
        self.name = name
        self._cond = Condition()
        self._exit = False
        self._pause = False

    def run(self):
        LOG.info("%s-%d started" % (self.name, self.threadID))
        while not self._exit:
            self._cond.acquire()
            if self._pause:
                self._cond.wait()
            self._cond.release()
            self.work()

    @property
    def cond(self):
        return self._cond

    @property
    def toExit(self):
        return self._exit

    @property
    def toPause(self):
        return self._pause

    def stop(self):
        self._exit = True
        LOG.info("%s-%d stoped" % (self.name, self.threadID))

    def pause(self):
        self._pause = True
        LOG.info("%s-%d paused" % (self.name, self.threadID))

    def resume(self):
        self._pause = False
        self._cond.acquire()
        self._cond.notify()
        self._cond.release()
        LOG.info("%s-%d resumed" % (self.name, self.threadID))

class Routine(Object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run(self):
        """What about this routing to do"""

class RoutinePool(MyThread):
    def __init__(self, delay):
        super(RoutinePool, self).__init__('RoutinePool', delay)
        self._routines = []
        self._delay = delay

    def work(self):
        for routine in self._routines:
            try:
                routine.run()
            except Exception, e:
                LOG.error(e)
        time.sleep(self._delay)

    def add(self, routine):
        if not isinstance(routine, Routine):
            raise TypeError('Must be an instance of Routine or its subclass')
        self._routines.append(routine)

    def remove(self, routine):
        if not isinstance(routine, Routine):
            raise TypeError('Must be an instance of Routine or its subclass')
        self._routines.remove(routine)

