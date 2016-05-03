#!/bin/env python

import os, sys, urllib

class mjf:

    InfoCode, WarningCode, ErrorCode = range(3)
        
    def __init__(self):
        # WarningCode etc values are what to report if that MJF key is absent
        self.featuredict = { 
                            'MACHINEFEATURES' : { 'hs06'        : self.WarningCode,
                                                  'total_cpu'   : self.ErrorCode,
                                                  'shutdowntime': self.InfoCode,
                                                  'grace_secs'  : self.InfoCode },

                            'JOBFEATURES' : { 'allocated_cpu'       : self.ErrorCode,
                                              'hs06_job'            : self.WarningCode,
                                              'shutdowntime_job'    : self.InfoCode,
                                              'grace_secs_job'      : self.InfoCode,
                                              'jobstart_secs'       : self.ErrorCode,
                                              'job_id'              : self.ErrorCode,
                                              'wall_limit_secs'     : self.ErrorCode,
                                              'cpu_limit_secs'      : self.ErrorCode,
                                              'max_rss_bytes'       : self.WarningCode,
                                              'max_swap_bytes'      : self.WarningCode,
                                              'scratch_limit_bytes' : self.WarningCode }
                           }
        self.exitcode = 0
        self.loglines = []
        self.logdict = {'Info'    : self.InfoCode, 
                        'Warning' : self.WarningCode, 
                        'Error'   : self.ErrorCode}
	self.logdictinv = {self.InfoCode: 'Info', self.WarningCode : 'Warning', self.ErrorCode : 'Error'}
        self.reportline = 'Probe executed successfully'

    def log(self, loglevel, logline):
        if self.logdict[loglevel] > self.exitcode : self.reportline = logline
        self.exitcode = max(self.exitcode, self.logdict[loglevel])
        self.loglines.append(loglevel.ljust(8) + ': ' + logline)

    def probe(self, featurevar):
        self.log('Info', 'Processing %s' % featurevar)
        proberoot = os.environ.get(featurevar)
        if proberoot != None : 
            self.log('Info', '%s=%s' % (featurevar, proberoot))
            onefeaturefound = False
            
            for featureName in self.featuredict[featurevar]:
                featureAbsentCode = self.featuredict[featurevar][featureName]
                
                featureValue = None
                try: 
                    featureValue = urllib.urlopen(proberoot + '/' + featureName).read()
                    if not featureValue : 
                      featureValue = 'None'

                    while featureValue[-1] == '\n': 
                      featureValue = featureValue[:-1]
                    
                    if not featureValue is None and featureValue != '':
                        onefeaturefound = True
                        self.log('Info', 'Key %s found with value %s' % (featureName, featureValue))
                    else :
                        featureValue = None
                        self.log('Info', 'Key %s found but value is empty' % featureName)

                except : 
                    featureValue = None

                if featureValue is None:
                  self.log(self.logdictinv[featureAbsentCode], 'Key %s absent (or empty)' % featureName)

            if not onefeaturefound: 
              self.log('Error', 'Environment variable %s set but cannot find any valid keys in there' % featurevar)

        else: 
            self.log('Error', 'Environment variable %s not set' % featurevar)

    def run(self):
        map(lambda x: self.probe(x), self.featuredict.keys())
        
        sys.stdout.write('%s %s\n\n' % (self.logdictinv[self.exitcode], self.reportline))
        map(lambda x: sys.stdout.write(x + '\n'), self.loglines)        
        sys.exit(self.exitcode)

if __name__ == '__main__' :
    mjf().run()
