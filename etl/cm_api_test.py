# -*- coding: utf-8 -*-

import etl_cm_api
import settings

import logging, json

logging.basicConfig(level=logging.DEBUG, \
    format='[%(asctime)s] %(name)-12s %(levelname)-8s %(message)s', \
    datefmt='%m-%d %H:%M', \
    filename=settings.log_file_path, \
    filemode='w')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

LOG = logging.getLogger(__name__)

# LOG.debug(etl_cm_api.get_host_status())

LOG.debug(etl_cm_api.get_cluster_resource())

# LOG.debug(etl_cm_api.get_cluster_resource_usage())

# LOG.debug(json.dumps(etl_cm_api.get_service_status()))

# LOG.debug(json.dumps(etl_cm_api.get_data_collector_volume()))

# LOG.debug(json.dumps(etl_cm_api.get_msg_queue_volume()))

# LOG.debug(json.dumps(etl_cm_api.get_data_statistics()))

# LOG.debug(json.dumps(etl_cm_api.get_vir_resource()))

# LOG.debug(json.dumps(etl_cm_api.get_vir_resource_status()))

# LOG.debug(json.dumps(etl_cm_api.get_user_statistics()))

