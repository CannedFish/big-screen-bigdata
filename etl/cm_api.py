# -*- coding: utf-8 -*-
import ssl
from cm_api.api_client import ApiResource

from settings import ca_file_path, cm_host_name

context = ssl.create_default_context(cafile=ca_file_path)
api = ApiResource(cm_host_name, use_tls=True, ssl_context=context)
# reses = api.query_timeseries('select cpu_percent where hostname=bigdata-120')
# for res in reses:
    # for ts in res.timeSeries:
        # for data in ts.data:
            # print data.timestamp, data.value, data.type

# Platform Statistics
# Physical resource
def get_host_total():
    pass

def get_cpu_mem_info_each_host():
    # Host Metrics
    # TODO: select cores, physical_memory_total where hostName rlike "bigdata-[0-9]+"
    pass

def get_storage_capacity_each_host():
    # Filesystem Metrics
    # TODO: capacity
    pass

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

