# -*- coding: utf-8 -*-

from datetime import datetime
import time
import logging

LOG = logging.getLogger(__name__)

import ssl
from cm_api.api_client import ApiResource

from settings import ca_file_path, cm_host_name, cm_usr, cm_pwd

clusters = [
    ['bigdata-5', 'bigdata-6', 'bigdata-7', 'bigdata-8', 'bigdata-9', 'bigdata-10', 'bigdata-11', 'bigdata-12', 'bigdata-13', 'bigdata-14', 'bigdata-15', 'bigdata-16', 'bigdata-17', 'bigdata-18', 'bigdata-19', 'bigdata-20', 'bigdata-21', 'bigdata-22'],
    ['bigdata-60', 'bigdata-61', 'bigdata-62', 'bigdata-63', 'bigdata-64', 'bigdata-65', 'bigdata-66', 'bigdata-67', 'bigdata-68'],
    []
]

def __cluster_belongs(hostname):
    if hostname in clusters[0]:
        return 0
    elif hostname in clusters[1]:
        return 1
    else:
        return 2

context = ssl.create_default_context(cafile=ca_file_path)
api = ApiResource(cm_host_name
        , username=cm_usr
        , password=cm_pwd
        , use_tls=True
        , ssl_context=context)
# reses = api.query_timeseries('select cpu_percent where hostname=bigdata-120')
# for res in reses:
    # for ts in res.timeSeries:
        # for data in ts.data:
            # print data.timestamp, data.value, data.type

def get_host_status():
    # NOTE: call every 5 minuts
    LOG.debug('get_host_status called')
    query = 'select health_good_rate where hostname rlike "bigdata-[0-9]+" and category=HOST'
    LOG.debug('query: %s' % query)

    result = [{
        'host_running': 0,
        'host_down': 0
    } for _ in range(5)]

    reses = api.query_timeseries(query) # from_time, to_time
    for res in reses:
        for ts in res.timeSeries:
            for data, _ in zip(ts.data, range(len(ts.data))):
                result[_]['timestamp'] = data.timestamp
                if data.value > 0.5:
                    result[_]['host_running'] += 1
                else:
                    result[_]['host_down'] += 1

    LOG.debug(result)
    return [{ \
        'timestamp': time.mktime(r['timestamp'].timetuple()),\
        'host_running': r['host_running'],\
        'host_down': r['host_down']\
    } for r in result]

def get_cluster_resource():
    LOG.debug('get_cluster_resource called')
    query = 'SELECT cores, physical_memory_total, total_capacity_across_directories WHERE hostname rlike "bigdata-[0-9]+" AND category=HOST'
    LOG.debug('query: %s' % query)

    result = [{
        'cluster': _,
        'nodes': 0,
        'cores': 0,
        'memory': 0,
        'disk': 0
    } for _ in range(3)]

    # from_time = datetime.fromtimestamp(time.time()-60)
    # Only get one minute ago
    reses = api.query_timeseries(query)
    for res in reses:
        data_size = len(res.timeSeries) / 3

        for core in res.timeSeries[0:data_size]:
            cluster = __cluster_belongs(core.metadata.entityName)
            result[cluster]['nodes'] += 1
            result[cluster]['cores'] += core.data[0].value

        for mem in res.timeSeries[data_size:data_size*2]:
            cluster = __cluster_belongs(mem.metadata.entityName)
            result[cluster]['memory'] += mem.data[0].value

        for disk in res.timeSeries[data_size*2:]:
            cluster = __cluster_belongs(disk.metadata.entityName)
            result[cluster]['disk'] += disk.data[0].value

    LOG.debug(result)
    return result


def get_cluster_resource_usage():
    LOG.debug('get_cluster_resource_usage called')
    
    result = [[{\
        'timestamp': None,\
        'cluster': _,\
        'cpu_percent': [],\
        'mem_used': 0,\
        'disk_used': 0,\
        'disk_input': [],\
        'disk_output': [],\
        'net_input': [],\
        'net_output': []\
    } for __ in range(5)] for _ in range(3)]

    # CPU, memory
    query = 'SELECT cpu_percent, physical_memory_used WHERE hostname RLIKE "bigdata-[0-9]+" AND category=HOST'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        data_size = len(res.timeSeries) / 2

        for cpu in res.timeSeries[0:data_size]:
            cluster = __cluster_belongs(cpu.metadata.entityName)
            for data, _ in zip(cpu.data, range(len(cpu.data))):
                result[cluster][_]['timestamp'] = data.timestamp
                result[cluster][_]['cpu_percent'].append(data.value)

        for mem in res.timeSeries[data_size:]:
            cluster = __cluster_belongs(cpu.metadata.entityName)
            for data, _ in zip(mem.data, range(len(mem.data))):
                result[cluster][_]['mem_used'] += data.value

    # Disk, Disk IO
    query = 'SELECT total_capacity_used_across_directories, total_write_bytes_rate_across_disks, total_read_bytes_rate_across_disks WHERE hostname RLIKE "bigdata-[0-9]+" AND category=HOST'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        data_size = len(res.timeSeries) / 3

        for disk in res.timeSeries[0:data_size]:
            cluster = __cluster_belongs(disk.metadata.entityName)
            for data, _ in zip(disk.data, range(len(disk.data))):
                result[cluster][_]['disk_used'] += data.value

        for disk_in in res.timeSeries[data_size:data_size*2]:
            cluster = __cluster_belongs(disk_in.metadata.entityName)
            for data, _ in zip(disk_in.data, range(len(disk_in.data))):
                result[cluster][_]['disk_input'].append(data.value)

        for disk_out in res.timeSeries[data_size*2:]:
            cluster = __cluster_belongs(disk_out.metadata.entityName)
            for data, _ in zip(disk_out.data, range(len(disk_out.data))):
                result[cluster][_]['disk_output'].append(data.value)

    # Net IO
    query = 'SELECT total_bytes_receive_rate_across_network_interfaces, total_bytes_transmit_rate_across_network_interfaces WHERE hostname RLIKE "bigdata-[0-9]+" AND category=HOST'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        data_size = len(res.timeSeries) / 2

        for net_in in res.timeSeries[0:data_size]:
            cluster = __cluster_belongs(net_in.metadata.entityName)
            for data, _ in zip(net_in.data, range(len(net_in.data))):
                result[cluster][_]['net_input'].append(data.value)

        for net_out in res.timeSeries[data_size:]:
            cluster = __cluster_belongs(net_out.metadata.entityName)
            for data, _ in zip(net_out.data, range(len(net_out.data))):
                result[cluster][_]['net_output'].append(data.value)

    LOG.debug(result)
    return [[\
        {
            'timestamp': time.mktime(metric['timestamp'].timetuple()),\
            'cluster': metric['cluster'],\
            'cpu_percent': sum(metric['cpu_percent'])/len(metric['cpu_percent']),\
            'mem_used': metric['mem_used'],\
            'disk_used': metric['disk_used'],\
            'disk_input': sum(metric['disk_input'])/len(metric['disk_input']),\
            'disk_output': sum(metric['disk_output'])/len(metric['disk_output']),\
            'net_input': sum(metric['net_input'])/len(metric['net_input']),\
            'net_output': sum(metric['net_output'])/len(metric['net_output'])\
        } for metric in c\
    ] for c in result]

def get_memory_total():
    pass

def get_storage_total():
    pass

# Profile
# NOTE: if possible, get these four profile through one tsquery
def get_cpu_usage():
    # NOTE: Host Metrics
    pass

def get_memory_usage():
    # NOTE: Host Metrics
    pass

def get_disk_io():
    # NOTE: Disk Metrics
    pass

def get_net_io():
    # NOTE: Network interface Metrics
    pass

# Abstract resource
def get_vcore_total():
    pass

# def get_memory_total():
    # pass

# def get_storage_total():
    # pass

# Abstract resource usage
def get_vcore_usage():
    pass

def get_vmemory_usage():
    pass

def get_storage_usage():
    pass

# Service report
def get_job_completed():
    pass

def get_job_failed():
    pass

# Job Statistics
def get_job_vcore_usage(job_id):
    pass

def get_job_vmemory_usage(job_id):
    pass

def get_job_during_time(job_id):
    pass

def get_job_result_status(job_id):
    pass

# User Statistics
def get_tenant_job_submitted(tenant_id):
    # job_id && resource usage(vcore, memory, during time)
    pass

def get_tenant_storage_usage(tenant_id):
    # at least including disk usage
    pass

# Service Statistics
def get_service_cpu_usage(service):
    pass

def get_service_memory_usage(service):
    pass

def get_service_disk_io(service):
    pass

def get_service_net_io(service):
    pass

