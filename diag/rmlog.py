import argparse, json, logging, logging.handlers, os.path, requests
import operator, platform, re, subprocess, sys, time
import re

from rmclasses import *

class RMLogFile:
    '''Parsed data from an RM log file'''

    # class names that appear in log files
    APP_ATTEMPT_IMPL = 'org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl'
    APP_IMPL = 'org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl'
    CONTAINER_IMPL = 'org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl'
    SCHED_NODE = 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode'

    # match lines like this:
    # container_1462948052533_0183_01_036931 Container Transitioned from ACQUIRED to RUNNING
    #
    CR_TRANSITION = '(\w+) Container Transitioned from (\w+) to (\w+)'
    RE_CR_TRANSITION = re.compile(CR_TRANSITION)

    # match lines like this:
    #   Updating application application_1466160025388_0174 with final state: KILLED
    #   application_1466160025388_0174 State change from RUNNING to KILLING
    #
    APP_TRANSITION = '(\w+) State change from (\w+) to (\w+)'
    RE_APP_TRANSITION = re.compile(APP_TRANSITION)

    # meta -- file metadata
    # data -- file content (several MB typically)
    #
    def __init__(self, a_meta, a_data):
        self.meta = a_meta
        self.logger = logging.getLogger("main")
        self.logger.debug('Parsing {0}, size = {1}, data length = {2}' \
                          .format(self.meta.name, self.meta.size, len(a_data)))

        # maps: {appId => Application}, {container_id => Container}, {address => Node}
        self.applications = {}
        self.containers = {}
        self.nodes = {}

        # parse lines into LogMsg objects
        self.logs = []

        # list of lines that failed to match the expected pattern
        failed_lines = []
        first = -1

        # list of YException objects
        self.errors, self.thr_dumps = [], []
        lines = a_data.splitlines()
        for i, line in enumerate(lines):
            try:
                x = LogMsg(line, i)
                self.logs.append(x)
                self.processLog(x)
                if failed_lines:
                    # reached end of exception stack trace or other group of
                    # non-matching lines, so reset
                    #
                    y = YException(failed_lines, first)
                    if y.isThrDump:
                        self.thr_dumps.append(y)
                    elif not y.ignore():
                        self.errors.append(y)
                    failed_lines, first = [], -1
            except ValueError as e:
                # RM got some sort of error, so need to skip lines till we get back
                # to normal format
                #
                #print 'failed line: {0}'.format(i)
                if not failed_lines:
                    failed_lines.append(lines[i - 1])    # the previous line is useful
                    failed_lines.append(line)
                    first = i - 1
                else:
                    failed_lines.append(line)

        msg = 'Read {0} lines, found {1} errors, {2} thread dumps, {3} apps'.format(
            len(self.logs), len(self.errors), len(self.thr_dumps), len(self.applications))
        self.logger.info(msg)

    def addApp(self, id):
        '''Return application with given id creating one if not found'''

        if id in self.applications:
            app = self.applications[id]
        else:
            # first time, so create a new app and add container to it
            app = Application(id)
            self.applications[id] = app
        return app

    def processLog(self, log):
        '''Process a single LogMsg object (one log line)'''

        if log.origin == self.APP_IMPL:
            m = self.RE_APP_TRANSITION.match(log.msg)
            if not m:                           # ignore for now
                return

            # retrieve app and add state transitions
            name, s_from, s_to = m.group(1), m.group(2), m.group(3)
            app = self.addApp(name)
            if not app.states:
                app.states.extend([s_from, s_to])
            else:
                last = app.states[-1] 
                if last == s_from:
                    app.states.append(s_to)
                else:
                    msg = 'Error: Last state = {0}, from = {1}, to = {1}' \
                        .format(last, s_from, s_to)
                    print msg
                    raise ValueError(msg)

        if log.origin == self.CONTAINER_IMPL:
            m = self.RE_CR_TRANSITION.match(log.msg)
            if not m:                           # ignore for now
                return

            # create container and add state transitions
            name, s_from, s_to = m.group(1), m.group(2), m.group(3)

            # if container exists, append state transition; otherwise, create new container
            if name in self.containers:
                c = self.containers[name]
                if not c.states:
                    c.states.extend([s_from, s_to])
                else:
                    last = c.states[-1] 
                    if last == s_from:
                        c.states.append(s_to)
                    else:
                        msg = 'Error: Last state = {0}, from = {1}, to = {1}' \
                            .format(last, s_from, s_to)
                        print msg
                        raise ValueError(msg)
            else:                                                   # create container
                c = Container(name)
                c.states.extend([s_from, s_to])
                self.containers[name] = c

            app = self.addApp(c.getFullAppId())
            app.addContainer(c)
            return

        if log.origin == self.SCHED_NODE:
            event = Event(log)
            if not hasattr(event, 'type'):         # did not find actual event
                return

            # create event and add to node
            addr = event.node_address
            if addr in self.nodes:
                node = self.nodes[addr]
            else:
                node = Node(addr)
                self.nodes[addr] = node
            node.events.append(event)

            # add node and container to each other
            c_id = event.container_id
            if c_id in self.containers:
                c = self.containers[c_id]
            else:                                                   # create container
                c = Container(c_id)
                self.containers[c_id] = c
            node.addContainer(c)
            if not c.node:
                c.node = node.host
            elif c.node != node.host:
                raise ValueError('container {0} nodes differ: {1} != {2}' \
                                 .format(c_id, c.node.host, node.host))

    def dumpErrors(self):
        '''Print list of errors'''

        for i, e in enumerate(self.errors):
            if e.ignore():
	        continue
            print '--------------- {0}'.format(i)
            print e

    def dumpApps(self):
        '''Print list of applications found'''

        print 'Found {0} applications:'.format(len(self.applications))
        for i, (app_id, app) in enumerate(self.applications.items()):
            print '--------------- {0}: {1} has {2} containers:' \
                .format(i, app_id, len(app.containers))
            for j, (_, container) in enumerate(app.containers.items()):
                print '  {0}: {1}'.format(j, container)

    def dumpNodes(self):
        '''Print list of nodes found'''

        print 'Found {0} nodes:'.format(len(self.nodes))
        for i, (address, node) in enumerate(self.nodes.items()):
            print '--------------- {0}: {1} has {2} containers:' \
                .format(i, address, len(node.containers))
            for j, (_, container) in enumerate(node.containers.items()):
                print '  {0}: {1}'.format(j, container.full_id)

