# -*- coding: utf-8 -*-

import logging

LOG = logging.getLogger(__name__)

import ssl
from cm_api.api_client import ApiResource

from settings import ca_file_path, cm_host_name, cm_usr, cm_pwd

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

# Platform Statistics
# Physical resource
def get_host_status():
    LOG.debug('get_host_status called')
    query = 'select health_good_rate where hostname rlike "bigdata-[0-9]+" and category=HOST'
    LOG.debug('query: %s' % query)

    reses = api.query_timeseries(query) # from_time, to_time
    for res in reses:
        for ts in res.timeSeries:
            LOG.debug(ts.metadata.attributes)
            for data in ts.data:
                LOG.debug(data.value)
    # TODO: fulfill data of phy_health

def __get_core_resource():
    # TODO: cores
    pass

def __get_mem_resource():
    # TODO: physical_memory_total
    pass

def __get_disk_capacity_resource():
    # TODO: total_capacity_across_directories
    pass

def get_cluster_resource():
    # TODO: fulfill data of cluster_resource, capacity maybe need to seperate with others
    LOG.debug('get_cluster_resource called')
    # query = 'select cores, physical_memory_total, capacity where hostname rlike "bigdata-[0-9]+" and category=HOST'
    query = 'select cores where hostname rlike "bigdata-[0-9]+" and category=HOST'
    LOG.debug('query: %s' % query)

    reses = api.query_timeseries(query)
    for res in reses:
        for ts in res.timeSeries:
            LOG.debug(ts.metadata.attributes)
            for data in ts.data:
                LOG.debug(data.value)
    # TODO: split info by cluster or releave this work to api caller
    __get_core_resource()
    __get_mem_resource()
    __get_disk_capacity_resource()

def get_cluster_resource_usage():
    # TODO: capacity
    LOG.debug('get_host_status called')
    query = 'select cpu_percent, physical_memory_used, capacity_used where hostname rlike "bigdata-[0-9]+" and category=HOST'
    LOG.debug('query: %s' % query)
    
    reses = api.query_timeseries(query)
    for res in reses:
        for ts in res.timeSeries:
            LOG.debug(ts.metadata.attributes)

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

