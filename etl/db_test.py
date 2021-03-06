# -*- coding: utf-8 -*-

import db
import settings

import logging

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

ret = db.getRows('phy_health')
if ret['success']:
    LOG.debug('Get phy_health rows: %s' % ret['data'])
else:
    LOG.debug('Get phy_health rows failed: %s' % ret['data'])

