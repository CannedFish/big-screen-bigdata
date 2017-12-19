# -*- coding: utf-8 -*-

from datetime import datetime
import time
import random
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
            result[cluster]['timestamp'] = time.mktime(core.data[0].timestamp.timetuple())
            result[cluster]['cores'] += core.data[0].value

        for mem in res.timeSeries[data_size:data_size*2]:
            cluster = __cluster_belongs(mem.metadata.entityName)
            result[cluster]['memory'] += mem.data[0].value

        for disk in res.timeSeries[data_size*2:]:
            cluster = __cluster_belongs(disk.metadata.entityName)
            result[cluster]['disk'] += disk.data[0].value

    return result


def get_cluster_resource_usage():
    # NOTE: call every 5 minuts
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
                # TODO: get from host status
                result[cluster][_]['health'] = 0
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

Service_Categroy = [
    ['Flume', 'Sqoop 2'],
    ['Kafka'],
    ['YARN (MR2 Included)', 'HDFS nameservice1', 'Hive', 'Spark 2', 'Zookeeper', 'HBase', 'HDFS']
]

def __service_cluster_belongs(serv_name):
    clusters_num = len(Service_Categroy)
    for _ in range(clusters_num):
        if serv_name in Service_Categroy[_]:
            return _

    return clusters_num

def get_service_status():
    result = {}

    query = 'SELECT health_good_rate WHERE category=SERVICE'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        for ts in res.timeSeries:
            if len(ts.data) == 0:
                continue
            ts_ret = []
            cluster = __service_cluster_belongs(ts.metadata.entityName)
            for data in ts.data:
                ts_ret.append({
                    'timestamp': time.mktime(data.timestamp.timetuple()),
                    'service_name': ts.metadata.entityName,
                    'cluster': cluster,
                    'health': 0 if data.value >= 0.5 else 1
                })
            result[ts.metadata.entityName] = ts_ret

    return result

def get_data_collector_volume():
    result = []

    query = 'SELECT event_received_rate_across_flume_sources WHERE category=CLUSTER'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        for ts in res.timeSeries:
            for data in ts.data:
                result.append({
                    'timestamp': time.mktime(data.timestamp.timetuple()),
                    'volume': data.value
                })

    return result

def get_msg_queue_volume():
    result = []

    query = 'SELECT kafka_bytes_received_rate_across_kafka_brokers, kafka_bytes_fetched_rate_across_kafka_brokers WHERE category=CLUSTER'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        volume_in = res.timeSeries[0]
        volume_out = res.timeSeries[1]
        for data_in, data_out in zip(volume_in.data, volume_out.data):
            result.append({
                'timestamp': time.mktime(data_in.timestamp.timetuple()),
                'volume_in': data_in.value,
                'volume_out': data_out.value
            })

    return result

def get_data_statistics():
    # TODO: count records from hdfs and hbase
    now = time.time()
    return [{\
            'timestamp': now-60*_,\
            'records': random.randint(0, 1000000)\
    } for _ in range(5)]

def get_vir_resource():
    result = []

    query = 'SELECT available_vcores_across_yarn_pools, available_memory_mb_across_yarn_pools, dfs_capacity_across_hdfss WHERE category=CLUSTER'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        vcore = res.timeSeries[0]
        vmem = res.timeSeries[1]
        hdfs = res.timeSeries[2]
        for c, m, h in zip(vcore.data, vmem.data, hdfs.data):
            result.append({
                'timestamp': time.mktime(c.timestamp.timetuple()),
                'vcores': c.value,
                'vmems': m.value,
                'hdfs_capacity': h.value,
            })

    return result

def get_vir_resource_status():
    result = []

    query = 'SELECT allocated_vcores_across_yarn_pools, allocated_memory_mb_across_yarn_pools, dfs_capacity_used_across_hdfss WHERE category=CLUSTER'
    LOG.debug('query: %s' % query)
    reses = api.query_timeseries(query)
    for res in reses:
        vcore = res.timeSeries[0]
        vmem = res.timeSeries[1]
        hdfs = res.timeSeries[2]
        for c, m, h in zip(vcore.data, vmem.data, hdfs.data):
            result.append({
                'timestamp': time.mktime(c.timestamp.timetuple()),
                'vcores_used': c.value,
                'vmems_used': m.value,
                'hdfs_used': h.value,
            })

    return result

# NOTE: For test, now is 30 day long
one_day_seconds = 60*60*24*30
def get_user_statistics():
    result = []

    query = 'SELECT allocated_vcore_seconds FROM YARN_APPLICATIONS'
    LOG.debug('query: %s' % query)
    from_time = datetime.fromtimestamp(time.time()-one_day_seconds)
    reses = api.query_timeseries(query, from_time)
    for res in reses:
        for ts in res.timeSeries:
            data = ts.metadata.attributes
            result.append({
                'timestamp': time.mktime(ts.data[0].timestamp.timetuple()),
                'job_id': data['entityName'],
                'user': data['user'],
                'vcore_seconds': data['allocated_vcore_seconds'],
                'memory_uesd': data['mb_millis'] if data['state'] == 'SUCCEEDED' else 0,
                'during_time': float(data['application_duration'])/1000.0,
                'status': data['state']
            })

    return result

