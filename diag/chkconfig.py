#!/usr/bin/python

# script to get hadoop configuration values and check them. Usage help:
#     python chkconfig.py --help
#

import argparse, json, logging, logging.handlers, os.path, requests
import operator, platform, re, subprocess, sys, time

import checks

# python 2.6 doesn't have NullHandler
try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
    logging.NullHandler = NullHandler

#logging.getLogger(__name__).addHandler(NullHandler())

class Checker:
    TIMEOUT = 5    # seconds for HTTP requests to timeout

    # LOG_FORMAT -- format of log messages
    # LOG_SIZE   -- max size of a log file in bytes
    # LOG_COUNT  -- max number of log file
    #
    LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s - %(message)s'
    LOG_SIZE = 2**20
    LOG_COUNT = 3

    # default path to file containing all known Hadoop config keys, one per line
    KEYS_FILE = 'keys.txt'

    def __init__(self):
        self.args = None    # parsed commandline arguments

    def onSandbox(self):
        '''Return true if on sandbox'''
        return 'dtbox' == platform.node()

    def getConfig(self):
        '''Get Configuration values for all known keys which should be in KEYS_FILE
        or the -k argument
        '''

        # map of configuration key to value; keys with no defined value will have
        # a value of None in this map
        #
        self.hadoop_config = {}

        # check if key file exists and is readable
        kfile = self.args.keys_file
        if not kfile:
            # location of key file not provided so use default
            kfile = os.path.dirname(os.path.realpath(__file__)) + '/' + self.KEYS_FILE
        if not os.path.exists(kfile):
           raise IOError('Error: {0} not found'.format(kfile))
        if not os.path.isfile(kfile):
           raise IOError('Error: {0} not a plain file'.format(kfile))
        if not os.access(kfile, os.R_OK):
           raise IOError('Error: {0} not readable'.format(kfile))

        # get all keys in file
        keys = [line.rstrip('\n') for line in open(kfile)]
        if not keys:
            raise ValueError('Error: No keys in {0}'.format(self.KEYS_FILE))
        else:
            self.logger.debug('Got {0} keys'.format(len(keys)))

        # This takes a long time ~15m for 700 keys but yields authoritative results (since
        # we get the values from the daemon) that can be cached for future use. See:
        #   http://stackoverflow.com/questions/34666416/get-a-yarn-configuration-from-commandline
        # for why other approaches like parsing configuration XML files may not yield
        # reliable results.
        #
        for key in keys:
            proc = subprocess.Popen(['hdfs', 'getconf', '-confKey', key],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            status = proc.returncode
            #print 'status = {0}'.format(proc.returncode)
            if status:                                 # non-zero status is failure
                self.hadoop_config[key] = None
                self.logger.debug('{0} no value'.format(key))
            else:                                      # success, we have a value
                result = stdout.strip()

                # for old versions of Hadoop we get a couple of lines of junk
                if result.startswith('OpenJDK 64-Bit Server VM warning'):
                    result = result.split('\n')[2]
                self.hadoop_config[key] = result
                self.logger.debug('{0} = {1}'.format(key, result))

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
            fmt = ('keys_file = {0}, log_file = {1}, debug = {2}')
            msg = fmt.format(a.keys_file, a.log_file, a.debug)
            self.logger.info(msg)

    def check(self):
        '''Sanity checks'''

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
        parser.add_argument('-f', '--log-file', help='Path to log file')
        parser.add_argument('-k',
                            '--keys-file', help='Path to file with hadoop config keys')

        self.args = parser.parse_args();

    def checkConfig(self):
        '''Get all Configuration values and perform sanity checks'''

        self.getConfig()
        rmcheck.check(self.config)

    def go(self):
        '''Main entry point'''

        self.parse_args()         # parse commandline arguments
        self.check()              # sanity checks
        self.init_logger()        # initialize logger
        self.checkConfig()

if __name__ == "__main__":
    c = Checker()
    c.go()
