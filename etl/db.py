# -*- coding: utf-8 -*-

from settings import db_server_host, db_server_port, db_server_app_key, \
        db_server_app_name

import requests
import base64
import json
import logging

LOG = logging.getLogger(__name__)

def do_get(url, headers):
    try:
        re = requests.get(url, headers=headers, timeout=2)
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.json()
        }
        LOG.info("GET %s" % url)
    except ValueError, e:
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.text
        }
        LOG.info("GET %s: %s" % (url, e))
    except Exception, e:
        ret = {
            'success': False,
            'data': e
        }
        LOG.error("GET %s: %s" % (url, e))
    return ret

def do_post(url, headers, data):
    try:
        re = requests.post(url, headers=headers, data=json.dumps(data), timeout=2)
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.json()
        }
        LOG.info("POST %s" % url)
    except ValueError, e:
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.text
        }
        LOG.info("POST %s: %s" % (url, e))
    except Exception, e:
        ret = {
            'success': False,
            'data': e
        }
        LOG.error("POST %s: %s" % (url, e))
    return ret

def do_put(url, headers, data):
    try:
        re = requests.put(url, headers=headers, data=json.dumps(data), timeout=2)
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.json()
        }
        LOG.info("PUT %s" % url)
    except ValueError, e:
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.text
        }
        LOG.info("PUT %s: %s" % (url, e))
    except Exception, e:
        ret = {
            'success': False,
            'data': e
        }
        LOG.error("PUT %s: %s" % (url, e))
    return ret

def do_delete(url, headers):
    try:
        re = requests.delete(url, headers=headers, timeout=2)
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.json()
        }
        LOG.info("DELETE %s" % url)
    except ValueError, e:
        ret = {
            'success': True,
            'status': re.status_code,
            'data': re.text
        }
        LOG.info("DELETE %s: %s" % (url, e))
    except Exception, e:
        ret = {
            'success': False,
            'data': e
        }
        LOG.error("DELETE %s: %s" % (url, e))
    return ret

app_header = {
    'Content-Type': 'application/json',
    'Authorization': db_server_app_key
}


def getRows(table, where=None):
    url = 'http://%s:%d/data/%s' % (db_server_host, db_server_port, table)
    if where is not None:
        url += '?where=%s' % base64.b64encode(where)
    
    return do_get(url, app_header)

def insertRows(table, data):
    """
    @data: {
        'columns': ['field1', 'field2'],
        'values': [
            ['value1', 'value2'],
            ['value3', 'value4']
        ]
    }
    """
    url = 'http://%s:%d/data/%s' % (db_server_host, db_server_port, table)
    return do_post(url, app_header, data)

def updateRow(table, row, data):
    """
    @data: {
        'values': {
            'field1': 'value1',
            'field2': 'value2'
        }
    }
    """
    url = 'http://%s:%d/data/%s/%s' % (db_server_host, db_server_port, table, row)
    return do_put(url, app_header, data)

def deleteRow(table, row):
    url = 'http://%s:%d/data/%s/%s' % (db_server_host, db_server_port, table, row)
    return do_delete(url)

