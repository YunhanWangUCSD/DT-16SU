#!/usr/bin/python

# script to get info about an application using Hadoop REST APIs. Usage help:
#     python chklogs.py --help
#

import argparse, json, logging, logging.handlers, os.path, requests
import operator, platform, re, subprocess, sys, time
from lxml import html, etree
from rmclasses import LogFileMeta
from rmlog import RMLogFile

# python 2.6 doesn't have NullHandler
try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
    logging.NullHandler = NullHandler

#logging.getLogger(__name__).addHandler(NullHandler())

# not available in some environments
#from bs4 import BeautifulSoup

class YarnInfo:
    TIMEOUT = 5    # seconds for HTTP requests to timeout

    # LOG_FORMAT -- format of log messages
    # LOG_SIZE   -- max size of a log file in bytes
    # LOG_COUNT  -- max number of log files
    #
    LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s - %(message)s'
    LOG_SIZE = 2**20
    LOG_COUNT = 3

    # config key for RM REST API
    RM_KEY = 'yarn.resourcemanager.webapp.address'

    # REST API paths
    INFO_PATH = 'ws/v1/cluster'
    JMX_PATH = 'jmx'
    LOGS_PATH = 'logs/'        # final slash is important
    APPS_PATH = INFO_PATH + '/apps'

    def __init__(self):
        self.args = None    # parsed commandline arguments
        self.cluster_info = {}
        self.getRMAddress()

    def onSandbox(self):
        '''Return true if on sandbox'''
        return 'dtbox' == platform.node()

    def getRMAddress(self):
        '''Get Resource Manager REST API address'''

        proc = subprocess.Popen(['hdfs', 'getconf', '-confKey', self.RM_KEY],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        status = proc.returncode
        #print 'status = {0}'.format(proc.returncode)
        if status:                      # non-zero status is failure
            fmt = 'Error: No value for key = {0}\n  stderr = {1}'
            msg = fmt.format(self.RM_KEY, stderr)
            raise ValueError(msg)

        result = stdout.strip()
        if not result:
            raise ValueError('Error: Blank value for {0}'.format(self.RM_KEY))

        # for old versions of Hadoop we get a couple of lines of junk
        if result.startswith('OpenJDK 64-Bit Server VM warning'):
            result = result.split('\n')[2]

        # logger not yet available
        #self.logger.debug('RM address = {0}'.format(result))

        self.rm_address = result

    def getClusterInfoJSON(self, path, add=True):
        '''Get YARN info from a REST API call; the response is expected to be JSON.
        If "add" is True, the result is added to the instance map with the path as key.
        '''

        self.logger.debug('RM address = {0}'.format(self.rm_address))
        url = 'http://{0}/{1}'.format(self.rm_address, path)

        headers={'Accept': 'application/json'}
        try:
            response = requests.get(url=url, headers=headers, timeout=self.TIMEOUT)
            result = response.json()

            if 'RemoteException' in result:
                self.logger.warn('Got RemoteException for {0}'.format(path))
                self.cluster_info[path] = None
                return None

            self.logger.debug('Result for path = "{0}":'.format(path))
            self.logger.debug(json.dumps(result, indent=4))
            if add:
                if self.cluster_info.get(path, None):
                    raise ValueError('Value for path = "{0}" already exists'.format(path))
                self.cluster_info[path] = result
            return result
        except requests.exceptions.Timeout:
            self.logger.error('getClusterInfoJSON: request timed out')

    def getClusterInfoText(self, path, add=True):
        '''Get YARN info from a REST API call. the response is assumed to be text.
        If "add" is True, the result is added to the instance map with the path as key.'''

        self.logger.debug('RM address = {0}'.format(self.rm_address))
        url = 'http://{0}/{1}'.format(self.rm_address, path)

        headers={'Accept': 'text/html'}
        try:
            response = requests.get(url=url, headers=headers, timeout=self.TIMEOUT)
            result = response.text

            # avoid logging if result is very long
            size = len(result)
            self.logger.debug('Result for path = "{0}" has length {1}'.format(path, size))
            if size <= 2048:
                self.logger.debug('Result for path = "{0}":'.format(path))
                self.logger.debug(result)

            if add:
                if self.cluster_info.get(path, None):
                    raise ValueError('Value for path = "{0}" already exists'.format(path))
                self.cluster_info[path] = result
            return result
        except requests.exceptions.Timeout:
            self.logger.error('getClusterInfoText: request timed out')

    def getLogFileList(self, path):
        '''Get log file list from a REST API call and store it'''

        # result is an HTML table, so we need to parse it
        r = self.getClusterInfoText(path, False)
        root = html.fromstring(r)
        items = root.xpath('//tr/td//text()')
        log_files = []
        for name, size, date in zip(*[iter(items)] * 3):
            name, size, date = name.strip(), size.strip(), date.strip()
            size = int(size.split(' ')[0])        # skip ' bytes' suffix
            self.logger.debug('{0} : {1} {2}'.format(name, size, date))
            log_files.append(LogFileMeta(name, size, date))

        # sort by timestamp in descending order
        log_files.sort(key=lambda x: x.ts, reverse=True)  
        self.cluster_info[path] = log_files

    # unused
    def findApp(self):
        '''Find info about app with given nam'''

        if not self.app_list:
            raise ValueException('Empty app list')
        self.logger.debug('Got {0} apps'.format(len(self.app_list)))

        # select records with desired name
        name = self.args.app_id
        list = [x for x in self.app_list if name == x['name']]
        if not list:
            msg = 'No applications found with name "{0}"'.format(name)
            logger.error(msg)
            return

        # process the most recent attempt to run the app
        slist = sorted(list, key=operator.itemgetter('startedTime'))
        
        if logger.isEnabledFor(logging.DEBUG):
            size = len(slist)
            for i, app in enumerate(slist):
                fmt = '%d: id = %s, startedTime = %s, state = %s'
                self.logger.debug(fmt % (i, app['id'], app['startedTime'],
                                          app['state']))
        app = slist[-1]

    def init_logger(self):
        ''' initialize logger based on parsed options '''

        self.logger = logging.getLogger("main")

        if self.args.quiet:
            self.logger.addHandler(logging.NullHandler)
        else:
            formatter = logging.Formatter(self.LOG_FORMAT)
            path = self.args.log_file
            fh = logging.handlers.RotatingFileHandler(path, maxBytes=self.LOG_SIZE,
                                                      backupCount=self.LOG_COUNT)
            fh.setFormatter(formatter)
            level = logging.DEBUG if self.args.debug else logging.INFO
            # fh.setLevel(level)    # not needed
            self.logger.addHandler(fh)
            self.logger.setLevel(level)

            # log arguments
            a = self.args
            fmt = ('app_id = {0}, log_file = {1}, debug = {2}')
            msg = fmt.format(a.app_id, a.log_file, a.debug)
            self.logger.info(msg)

    def check(self):
        '''Sanity checks'''

        # need app_id
        if not self.args.app_id:
            raise ValueError('Error: blank application id')
            self.args.app_id = self.args.app_id.strip()
            if not self.args.app_id:
                raise ValueError('Error: blank application id')

        # if -q is absent, need path to log file
        if not self.args.quiet:
            if not self.args.log_file:
                raise ValueError('Need path to log file (or quiet option)')

            # have path to log file
            self.args.log_file = self.args.log_file.strip()
            if not self.args.log_file:
                raise ValueError('Path to log file is blank')

    def parse_args(self):
        '''Parse commandline arguments'''
        parser = argparse.ArgumentParser()

        # boolean options
        group = parser.add_mutually_exclusive_group()
        group.add_argument('-d', '--debug', help='Enable debug logging',
                           action='store_true')
        group.add_argument('-q', '--quiet', help='Suppress logging',
                           action='store_true')

        # options with arguments 
        parser.add_argument('-a', '--app-id', help='Application Id')
        parser.add_argument('-m', '--am-logs',
                            help='path to save App Master and container logs')
        parser.add_argument('-f', '--log-file', help='Path to log file')

        self.args = parser.parse_args();

    def readRMLogFile(self):
        '''Retrieve and parse current RM log file'''

        # get value of 'InputArguments' in JMX data
        jmx = self.cluster_info[self.JMX_PATH]['beans']
        if not jmx:
            raise ValueError('No JMX data')
        key, args = 'InputArguments', None
        for obj in jmx:
            if 'java.lang:type=Runtime' == obj['name']:
                args = obj[key]
                break
        if not args:
            raise ValueError('No value for key = {0}'.format(key))

        # get name of YARN log file
        # there are multiple occurrences, so pick the last one
        #
        log_file = None
        for v in reversed(args):
            if v.startswith('-Dyarn.log.file='):
                self.logger.debug('Found v = "{0}"'.format(v))
                log_file = v.split('=')[1]
                break
        if not log_file:
            raise ValueError('No log file name found')
        self.logger.info('log_file = "{0}"'.format(log_file))
        self.log_file = log_file

        # find element in our list of log files
        log_files = self.cluster_info[self.LOGS_PATH]
        if not log_files:
            raise ValueError('No log_files')

        # list of indices where the name occurs
        pos = [i for i, x in enumerate(log_files) if x.name == log_file]
        if not pos:
            raise ValueError('"{0}" not in log_files'.format(log_file))
        file_meta = log_files[pos[0]]
        self.logger.debug('"{0}" is at positions: {1}; size = {2}, timestamp = {3}'
                          .format(log_file, pos, file_meta.size, file_meta.ts))

        # get file content (LOGS_PATH ends with a slash)
        path = '{0}{1}'.format(self.LOGS_PATH, log_file)
        log_data = self.getClusterInfoText(path, False)
        self.logger.info('log_data length = {0}'.format(len(log_data)))

        # parse log file
        self.log_file = RMLogFile(file_meta, log_data)

        # print results
        self.log_file.dumpErrors()
        print '----------------------------------------------'
        app_info = self.cluster_info.get(self.app_path, None)['app']
        print json.dumps(app_info, indent=4)
        app = self.log_file.applications[self.args.app_id]
        app.dump()

        #log_file.dumpApps()
        #log_file.dumpNodes()

    def getAMLog(self):
        '''Get App Master logs'''
        name = 'dt.log'

        # if destination path is a directory, append file name
        path = self.args.am_logs
        if os.path.isdir(path):
            if '/' != path[-1]:
                path += '/'
            path += name
        self.logger.info('Storing AM log at: {0}'.format(path))

        # construct source URL
        app_info = self.cluster_info.get(self.app_path, None)['app']
        amContainerLogs = app_info['amContainerLogs']
        url = amContainerLogs +  '/dt.log/?start=0'
        self.logger.info('Getting AM log from: {0}'.format(url))

        r = requests.get(url, stream=True)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=4096): 
                if chunk:          # filter out keep-alive new chunks
                    f.write(chunk)

    def go(self):
        '''Main entry point'''

        self.parse_args()         # parse commandline arguments
        self.check()              # sanity checks
        self.init_logger()        # initialize logger
        self.getClusterInfoJSON(self.INFO_PATH)
        self.getClusterInfoJSON(self.JMX_PATH)
        self.app_path = self.APPS_PATH + "/" + self.args.app_id
        app = self.getClusterInfoJSON(self.app_path)
        if not app:
            raise ValueError('Failed to find {0}'.format(self.args.app_id))

        self.getLogFileList(self.LOGS_PATH)
        self.readRMLogFile()
        if self.args.am_logs:
            self.getAMLog()

        #info = self.getClusterInfo('ws/v1/cluster/apps')
        #self.app_list = info['apps']['app']      # should be an array of objects
        #self.findApp()

if __name__ == "__main__":
    y = YarnInfo()
    y.go()
