#!/bin/env python

import os, sys, urllib

class mjf:
    
    def __init__(self):
        self.featuredict = {'MACHINEFEATURES' : ['hs06', 'shutdowntime', 'jobslots', 'phys_cores', 'log_cores', 'shutdown_command'], 
                            'JOBFEATURES' : ['cpufactor_lrms', 'cpu_limit_secs_lrms', 'cpu_limit_secs', 'wall_limit_secs_lrms', 'wall_limit_secs', 'disk_limit_GB', 'jobstart_secs', 'mem_limit_MB', 'allocated_CPU', 'shutdowntime_job'] 
                            }
        self.exitcode = 0
        self.loglines = []
        self.logdict = {'Info' : 0, 
                        'Warning' : 1, 
                        'Error' : 2}
        self.logdictinv = {v : k.upper() for k, v in self.logdict.items()}
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
            wnfeatures = os.listdir(proberoot)
            prfeatures = self.featuredict[featurevar]
            
            for feature in wnfeatures: 
                featureval = None
                try: 
                    featureval = urllib.urlopen(proberoot+os.sep+feature).read()
                    if not featureval : featureval = 'None'
                    while featureval[-1] == '\n' : featureval = featureval[:-1]
                    if feature in prfeatures : 
                        onefeaturefound = True
                        self.log('Info', 'Feature %s=%s found' % (feature, featureval))
                    else :
                        self.log('Warning', 'Feature %s=%s available on workernode but not in specification' % (feature, featureval))
                except : 
                    self.log('Error', 'Cannot read %s feature entry %s' % (featurevar, feature))

            if not onefeaturefound : self.log('Error', 'Environment variable %s set but cannot find valid features in there' % featurevar)
            else:
                wnfeaturesset = set(wnfeatures)
                for feature in [feature for feature in prfeatures if feature not in wnfeaturesset] : 
                    self.log('Info', 'Feature %s from specification not found on the worker node' % feature)
        else: 
            self.log('Error', 'Environment variable %s not set' % featurevar)

    def run(self):
        map(lambda x: self.probe(x), self.featuredict.keys())
        
        sys.stdout.write('%s %s\n\n' % (self.logdictinv[self.exitcode], self.reportline))
        map(lambda x: sys.stdout.write(x + '\n'), self.loglines)        
        sys.exit(self.exitcode)

if __name__ == '__main__' :
    mjf().run()
