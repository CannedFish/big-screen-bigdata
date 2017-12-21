# -*- coding: utf-8 -*-

import etl_cm_api as api
import db

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

class Routine(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run(self):
        """What about this routing to do"""

class RoutineQueue(MyThread):
    def __init__(self, delay):
        super(RoutineQueue, self).__init__('RoutineQueue', delay)
        self._routines = []
        self._delay = delay

    def work(self):
        for routine in self._routines:
            try:
                ret = routine.run()
                LOG.debug('%s runs %s: %s' % (type(routine).__name__, \
                        'OK' if ret['success'] else 'Failed', \
                        ret['data']))
            except Exception, e:
                LOG.error(e)
        time.sleep(self._delay)

    def add(self, routine):
        if not isinstance(routine, Routine):
            raise TypeError('Must be an instance of Routine or its subclass')
        if routine in self._routines:
            LOG.warning('%s has been added.' % type(routine).__name__)
            return
        self._routines.append(routine)
        LOG.info("%s has been added." % type(routine).__name__)

    def remove(self, routine):
        if not isinstance(routine, Routine):
            raise TypeError('Must be an instance of Routine or its subclass')
        if routine not in self._routines:
            LOG.warning('%s has not been added.' % type(routine).__name__)
            return
        self._routines.remove(routine)
        LOG.info("%s has been removed." % type(routine).__name__)

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class AbsSingleton(abc.ABCMeta, Singleton):
    pass

class HostStatusRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = map(lambda x: [x['timestamp'], x['host_running'], x['host_down']], \
                api.get_host_status())
        LOG.debug(val)
        return db.insertRows('phy_health', {\
            'columns': ['timestamp', 'host_running', 'host_down'],\
            'values': val\
        })

class ClusterResourceRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = map(lambda x: [x['timestamp'], x['cluster'], x['nodes'], x['cores'], \
                x['memory'], x['disk']], api.get_cluster_resource())
        LOG.debug(val)
        return db.insertRows('cluster_resource', {\
            'columns': ['timestamp', 'cluster', 'nodes', 'cores', 'memory', 'disk'],\
            'values': val\
        })

class ClusterResourceUsageRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        ret = api.get_cluster_resource_usage()
        val = []
        for _ in ret:
            val.extend([[x['timestamp'], x['cluster'], x['cpu_percent'], \
                x['mem_used'], x['disk_used'], x['disk_input'], x['disk_output'], \
                x['net_input'], x['net_output'], x['health']] for x in _])
        LOG.debug(val)
        return db.insertRows('cluster_status', {\
                'columns': ['timestamp', 'cluster', 'cpu_percent', 'mem_used', \
                    'disk_used', 'disk_input', 'disk_output', 'net_input', \
                    'net_output', 'health'],
                'values': val
        })

class ServiceStatusRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        ret = api.get_service_status()
        val = []
        for _ in ret.itervalues():
            val.extend([[x['timestamp'], x['service_name'], x['cluster'], x['health']] \
                    for x in _])
        LOG.debug(val)
            
        return db.insertRows('service_status', {\
            'columns': ['timestamp', 'service_name', 'cluster', 'health'],\
            'values': val\
        })

class DataCollectroVolumeRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = [[x['timestamp'], x['volume']] for x in api.get_data_collector_volume()]
        LOG.debug(val)
        return db.insertRows('data_collector_volume', {\
            'columns': ['timestamp', 'volume'],\
            'values': val\
        })

class MsgQueueVolumeRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = [[x['timestamp'], x['volume_in'], x['volume_out']] \
                for x in api.get_msg_queue_volume()]
        LOG.debug(val)
        return db.insertRows('msg_queue_volume', {\
            'columns': ['timestamp', 'volume_in', 'volume_out'],\
            'values': val\
        })

class DataStatisticsRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = [[x['timestamp'], x['records']] for x in api.get_data_statistics()]
        LOG.debug(val)
        return db.insertRows('data_statistics', {\
            'columns': ['timestamp', 'records'],\
            'values': val
        })

class VirResourceRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = [[x['timestamp'], x['vcores'], x['vmems'], x['hdfs_capacity']] \
                for x in api.get_vir_resource()]
        LOG.debug(val)
        return db.insertRows('vir_resource', {\
            'columns': ['timestamp', 'vcores', 'vmems', 'hdfs_capacity'],\
            'values': val\
        })

class VirResourceStatusRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = [[x['timestamp'], x['vcore_used'], x['vmem_used'], x['hdfs_used']] \
                for x in api.get_vir_resource_status()]
        LOG.debug(val)
        return db.insertRows('vir_res_status', {\
            'columns': ['timestamp', 'vcore_used', 'vmem_used', 'hdfs_used'],\
            'values': val\
        })

class UserStatisticsRoutine(Routine):
    __metaclass__ = AbsSingleton

    def run(self):
        val = [[x['timestamp'], x['job_id'], x['user'], x['vcore_seconds'], \
                x['memory_used'], x['during_time'], x['status']] \
                for x in api.get_user_statistics()]
        LOG.debug(val)
        return db.insertRows('user_statistics', {\
            'columns': ['timestamp', 'job_id', 'user', 'vcore_seconds', \
                        'memory_used', 'during_time', 'status'],
            'values': val
        })

