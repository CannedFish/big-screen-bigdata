# -*- coding: utf-8 -*-

from routines import *
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

# LOG.debug(HostStatusRoutine().run())

# LOG.debug(ClusterResourceRoutine().run())

# LOG.debug(ClusterResourceUsageRoutine().run())

# LOG.debug(ServiceStatusRoutine().run())

# LOG.debug(DataCollectroVolumeRoutine().run())

# LOG.debug(MsgQueueVolumeRoutine().run())

# LOG.debug(DataStatisticsRoutine().run())

# LOG.debug(VirResourceRoutine().run())

# LOG.debug(VirResourceStatusRoutine().run())

LOG.debug(UserStatisticsRoutine().run())

