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

def main():
    # five minutes routines
    five_minutes_queue = RoutineQueue(5*60)

    five_minutes_queue.add(HostStatusRoutine())
    five_minutes_queue.add(ClusterResourceUsageRoutine())
    five_minutes_queue.add(ServiceStatusRoutine())
    five_minutes_queue.add(DataCollectroVolumeRoutine())
    five_minutes_queue.add(MsgQueueVolumeRoutine())
    five_minutes_queue.add(VirResourceStatusRoutine())

    five_minutes_queue.start()

    # one day routines
    one_day_queue = RoutineQueue(24*60*60)

    one_day_queue.add(ClusterResourceRoutine())
    one_day_queue.add(DataStatisticsRoutine())
    one_day_queue.add(VirResourceRoutine())
    one_day_queue.add(UserStatisticsRoutine())

    one_day_queue.start()

if __name__ == '__main__':
    main()

