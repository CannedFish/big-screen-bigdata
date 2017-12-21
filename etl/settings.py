# -*- coding: utf-8 -*-

from os import path
import re

# Log
log_file_path = '/var/log/big_screen_bigdata.log'

# Cloudera

cm_usr = 'admin'
cm_pwd = 'admin'
# pem file for ssl
ca_file_path = '/home/crack/cmhost.pem'
# hostname for cloudera manager
cm_host_name = 'bigdata-120'

# DB
db_server_host = '10.11.200.28'
db_server_port = 9000
db_server_app_name = 'visualization_bigdata'
db_server_app_key = 'rMjuL9Z4F2CmOwFJnRKk08VFq2DmWiln'

# Get configuration from the configure file
conf_file_path = '/etc/bs_bigdata_etl.conf'
if path.exists(conf_file_path):
    with open(conf_file_path, 'r') as f:
        conf = globals()
        for line in f:
            if not re.match('\s*#', line) \
                    and not re.match('\s+', line):
                l = re.split('=', line)
                # NOTE: handle special keys
                key = l[0].strip()
                if key == 'db_server_port':
                    conf[key] = int(l[1].strip())
                else:
                    conf[key] = l[1].strip()

