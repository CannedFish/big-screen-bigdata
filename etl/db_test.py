# -*- coding: utf-8 -*-

import db

import logging

LOG = logging.getLogger(__name__)

ret = db.getRows('phy_health')
if ret['success']:
    LOG.debug('Get phy_health rows: %s' % ret['data'])
else:
    LOG.debug('Get phy_health rows failed: %s' % ret['data'])

