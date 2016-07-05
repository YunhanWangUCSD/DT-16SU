import re
from dateutil.parser import parse

# some useful classes for YARN Resource Manager log messages

class LogFileMeta:
    '''Meta information about an RM log file'''

    # name -- name of log file
    # size -- size of log file
    # date -- timestamp of log file as a string
    #
    def __init__(self, name, size, date):
        self.name, self.size = name, size
        self.ts = parse(date.strip())

class LogMsg:
    '''Base class for YARN Resource Manager log message; data from a single
    log line
    '''

    # RM log levels
    RM_DEBUG, RM_INFO, RM_WARN, RM_ERROR = range(1, 5)

    # Container states
    CR_NEW, CR_ACQUIRED, CR_ALLOCATED, CR_RUNNING, CR_COMPLETED, CR_FINISHED = range(1, 7)

    # YARN cluster id
    cluster_id = None


    # regex for a log line, example:
    # 2016-05-24 02:53:21,384 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler: Null container completed...
    #

    RE_DATE = '(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})'
    RE_LEVEL = '(ERROR|WARN|INFO|DEBUG)'
    RE_ORIGIN = '((?:\w|\.|\$)+)'                       # class names can have '$'
    RE_MSG = '(.+)'

    RE_STR = '\A{0} {1} {2}: {3}\Z'.format(RE_DATE, RE_LEVEL, RE_ORIGIN, RE_MSG)
    #print 'RE_STR = {0}'.format(RE_STR)

    RE_LINE = re.compile(RE_STR)

    # line -- log line
    def __init__(self, line, line_num=0):
        m = self.RE_LINE.match(line)
        if not m:
            raise ValueError('No match for line {0}: "{1}"'.format(line_num, line))

        # ts -- timestamp
        # level -- log level
        # origin -- the java class from which the message originated, e.g.
        #   org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl
        # msg -- message
        #
        self.ts     = m.group(1)
        self.level  = m.group(2)
        self.origin = m.group(3)
        self.msg    = m.group(4)

class Container:
    '''A YARN container'''

    # Container names have one of 2 forms:
    #     container_{cluster}_{appId}_{attempt}_{id}
    #     container_{epoch}_{cluster}_{appId}_{attempt}_{id}
    # for example:
    #     container_1462948052533_0036_01_022464
    #     container_e35_1465495186350_2224_01_000001
    #
    # where {cluster}, {appId}, {attempt}, {id} are respectively the cluster,
    # application, attempt and container ids; {epoch} is newly introduced.
    #
    PREFIX = 'container_'
    SUFFIX = '(\d+)_(\d+)_(\d+)_(\d+)'
    RE_CONTAINER_ID = re.compile(PREFIX + SUFFIX)
    RE_CONTAINER_ID_E = re.compile(PREFIX + 'e(\d+)_' + SUFFIX)

    def __str__(self):
        '''Convert to string'''

        result = 'full_id: {0}, node = {1}, states: {2}' \
            .format(self.full_id, self.node, ','.join(self.states))
        return result

    def __init__(self, name):
        ''' Container names have one of 2 forms:
            container_{cluster}_{attempt}_{id}
            container_{epoch}_{cluster}_{attempt}_{id}
        for example:
            container_1462948052533_0036_01_022464
            container_e35_1465495186350_2224_01_000001
        '''

        # try to match either form
        m = self.RE_CONTAINER_ID.match(name)
        if m:
            i = 1
        else:
            m = self.RE_CONTAINER_ID_E.match(name)
            if not m:
                msg = 'Bad container id {0}'.format(name)
                print msg
                raise ValueError(msg)

            self.epoch   = m.group(1)
            i = 2

        self.cluster_id = m.group(i)
        self.app_id     = m.group(i + 1)
        self.attempt_id = m.group(i + 2)
        self.id         = m.group(i + 3)
        self.full_id    = name
        self.node       = None    # name of node hosting this container

        # list of states container has transitioned through
        self.states = []

    def getFullAppId(self):
        '''Return synthesized application id '''
        return 'application_{0}_{1}'.format(self.cluster_id, self.app_id)

class Application:
    '''A YARN Application'''

    # application id has the form:
    #     application_1462948052533_0183
    # where the first number is the cluster id and the second is the actual app id.
    #
    RE_APPLICATION_ID = re.compile('application_(\d+)_(\d+)')

    def __init__(self, name):
        m = self.RE_APPLICATION_ID.match(name)
        if not m:
            raise ValueError('Bad application id {0}'.format(name))

        # NOTE: Multiple cluster ids may be present in a log file
        self.cluster_id = m.group(1)
        self.id = m.group(2)
        self.full_id = name

        # containers, keyed by id
        self.containers = {}

        # states that the application transitions through:
        # RUNNING, KILLING, FINAL_SAVING etc.
        #
        self.states = []

    def addContainer(self, container):
        '''Add new container if not already present'''

        if not container.full_id in self.containers:
            self.containers[container.full_id] = container

    def dump(self):
        '''Print application info'''
        print '--------------- {0} has {1} containers:' \
            .format(self.full_id, len(self.containers))
        print 'states = {0}'.format(','.join(self.states))
        for j, (_, container) in enumerate(self.containers.items()):
            print '  {0}: {1}'.format(j, container)

class Attempt:
    '''An Application attempt'''

    # an attempt id has the form:
    #     appattempt_1462948052533_0183_000001
    # where the first 2 numbers are the cluster and app id, the third is the attempt id
    #
    RE_APPLICATION_ID = re.compile('appattempt_(\d+)_(\d+)_(\d+)')

    def __init__(self, name):
        m = self.RE_APPLICATION_ID.match(name)
        if not m:
            raise ValueError('Bad attempt id {0}'.format(name))

        # NOTE: Multiple cluster ids may be present in a log file
        self.cluster_id = m.group(1)
        self.app_id = m.group(2)
        self.id = m.group(3)
        self.full_id = name

    def getFullAppId(self):
        '''Get the full application id'''
        return 'application_{0}_{1}'.format(self.cluster_id, self.app_id)

class Node:
    '''A node in a Hadoop cluster'''

    RE_ADDRESS = re.compile('\A\S+\Z')

    def __init__(self, addr):
        m = self.RE_ADDRESS.match(addr)
        if not m:
            raise ValueError('Bad host address {0}'.format(addr))

        self.address = addr
        if ':' not in addr:
            raise ValueError('No port in address')

        idx = addr.rindex(':')
        self.host, self.port = addr[:idx], int(addr[idx + 1:])

        # container allocation and release events
        self.events = []

        # list of containers on this node
        self.containers = {}

    def addContainer(self, container):
        '''Add new container if not already present'''

        if not container.full_id in self.containers:
            self.containers[container.full_id] = container

class Event:
    '''A container allocation or release event'''

    # match lines like this:
    #  Assigned container container_1465376707429_10394_01_043816 of capacity <memory:1024, vCores:1> on host node29.morado.com:8041, which has 17 containers, <memory:53760, vCores:17> used and <memory:151040, vCores:7> available after allocation
    #
    CR_ASSIGNED = ('Assigned container (\w+) of capacity <memory:(\d+), vCores:(\d+)> '
                  'on host ([-a-zA-Z0-9.:]+), which has (\d+) containers, '
                  '<memory:(\d+), vCores:(\d+)> used and <memory:(\d+), vCores:(\d+)> '
                  'available after allocation')

    RE_CR_ASSIGNED = re.compile(CR_ASSIGNED)

    # match lines like this:
    # Released container container_1465376707429_12480_01_007152 of capacity <memory:1024, vCores:1> on host node24.morado.com:8041, which currently has 13 containers, <memory:62464, vCores:13> used and <memory:142336, vCores:11> available, release resources=true
    #
    CR_RELEASED = ('Released container (\w+) of capacity <memory:(\d+), vCores:(\d+)> '
                  'on host ([-a-zA-Z0-9.:]+), which currently has (\d+) containers, '
                  '<memory:(\d+), vCores:(\d+)> used and <memory:(\d+), vCores:(\d+)> '
                  'available, release resources=true')

    RE_CR_RELEASED = re.compile(CR_RELEASED)

    ASSIGNED, RELEASED = 1, 2        # event types

    def __init__(self, log):
        m = self.RE_CR_ASSIGNED.match(log.msg)
        if m:
            self.type = self.ASSIGNED
        else:
            m = self.RE_CR_RELEASED.match(log.msg)
            if not m:                           # ignore for now
                print 'Failed to match: {0}'.format(log.msg)
                return
            self.type = self.RELEASED

        self.container_id      = m.group(1)
        self.container_mem     = m.group(2)
        self.container_vcores  = m.group(3)
        self.node_address      = m.group(4)
        self.node_n_containers = m.group(5)
        self.node_used_mem     = m.group(6)
        self.node_used_vcores  = m.group(7)
        self.node_avail_mem    = m.group(8)
        self.node_avail_vcores = m.group(9)
        self.ts = log.ts;

class YException:
    '''Yarn Exception log or thread dump'''

    RE_THR_DUMP = re.compile('(\d+) active threads')    # first line of thread dump

    # lines -- list of lines related to the exception
    # line_num -- line number of first line of exception
    #
    def __init__(self, lines, line_num):
        if not lines:
            raise ValueError('Empty line list')
        self.lines = lines
        self.line_range = [line_num, line_num + len(lines)]
        self.isThrDump = True if self.RE_THR_DUMP.match(self.lines[0]) else False

    def __repr__(self):
        return 'lines: {0}\n'.format(self.line_range) + '\n'.join(self.lines)

    def ignore(self):
        '''return True if we want to ignore this exception, False otherwise

        Ignore chunks that begin with any of these fragments:

        ---------------------
              capacity = 1.0 [= (float) configuredCapacity / 100 ]
              asboluteCapacity = 1.0 [= parentAbsoluteCapacity * capacity ]
              maxCapacity = 1.0 [= configuredMaxCapacity ]
              absoluteMaxCapacity = 1.0 [= 1.0 maximumCapacity undefined, (parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]
              userLimit = 100 [= configuredUserLimit ]

        ---------------------
            /************************************************************
            SHUTDOWN_MSG: Shutting down ResourceManager at dtbox/127.0.1.1

        ---------------------
            /************************************************************
            STARTUP_MSG: Starting ResourceManager


        '''
        line =  self.lines[1]
        if '/*************************************************' in line:
            return True
        if 'capacity = ' in line:
            return True

        return False

if __name__ == "__main__":
    line = '2016-05-24 02:53:21,384 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler: Null container completed...'
    x = LogMsg(line)
    print 'match:\n  date = {0}\n  level = {1}\n  origin = {2}\n  msg = {3}'.format(
        x.ts, x.level, x.origin, x.msg)

    line = '2016-05-24 02:53:22,571 INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_1462948052533_0183_01_036931 Container Transitioned from ACQUIRED to RUNNING'
    x = LogMsg(line)
    print 'match:\n  date = {0}\n  level = {1}\n  origin = {2}\n  msg = {3}'.format(
        x.ts, x.level, x.origin, x.msg)
