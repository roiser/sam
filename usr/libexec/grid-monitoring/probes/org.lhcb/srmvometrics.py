#!/usr/bin/env python

##############################################################################
#
# NAME:        srmvometrics.py
#
# FACILITY:    SAM (Service Availability Monitoring)
#
# COPYRIGHT:
#         Copyright (c) 2009, Members of the EGEE Collaboration.
#         http://www.eu-egee.org/partners/
#         Licensed under the Apache License, Version 2.0.
#         http://www.apache.org/licenses/LICENSE-2.0
#         This software is provided "as is", without warranties
#         or conditions of any kind, either express or implied.
#
# DESCRIPTION:
#
#         VO-specific Nagios SRM metrics.
#
# AUTHORS:     Nicolo Magini, CERN
# AUTHORS:     Alessandro Di Girolamo, CERN
# AUTHORS:     Konstantin Skaburskas, CERN
# AUTHORS:     Roberto Santinelli, CERN
#
# CREATED:     23-Jul-2010
#
# NOTES:
#
##############################################################################

"""
Nagios SRM metrics.

Nagios SRM metrics.

Nicolo Magini <nicolo.magini@cern.ch>, 
Alessandro Di Girolamo <Alessandro.Di.Girolamo@cern.ch>
Roberto Santinelli <roberto.santinelli@cern.ch>
CERN IT Experiment Support
SAM (Service Availability Monitoring)
"""

import os
import sys
import getopt
import time #@UnresolvedImport
import signal
import commands
import errno
import re
import urllib2
import simplejson
import pickle
import datetime

try:
    from gridmon import probe
    from gridmon import utils as samutils
    from gridmon import gridutils
    import lcg_util
    import gfal
except ImportError,e:
    summary = "UNKNOWN: Error loading modules : %s" % (e)
    sys.stdout.write(summary+'\n')
    sys.stdout.write(summary+'\nsys.path: %s\n'% str(sys.path))
    sys.exit(3)


class SRMVOMetrics(probe.MetricGatherer) :
    """A Metric Gatherer specific for SRM."""

    # Service version(s)
    svcVers = ['1', '2'] # NOT USED YET
    svcVer  = '2'

    # The probe's author name space -- CHANGE it to your ns
#    ns = 'org.atlas'
    ns = 'org.lhcb'

    _timeouts = {'ldap_connect'   : 0, 
                 'ldap_timelimit' : 10,
                 'srm_connect'    : 120,
                 'srm_operation'  : 0}
    
    _ldap_url  = "ldap://sam-bdii.cern.ch:2170"
    
    probeinfo = { 'probeName'      : ns+'.SRM-Probe',
                  'probeVersion'   : '1.0',
                  'serviceVersion' : '1.*, 2.*'}
    # Metrics' info
    _metrics = { 
               'GetSURLs' : {'metricDescription': "Get full SRM endpoints and storage areas from BDII.",
                             'cmdLineOptions'   : ['ldap-uri=',
                                                   'ldap-timeout='],
                             'cmdLineOptionsReq' : [],
                             'metricChildren'   : ['LsDir','Put','Ls','GetTURLs','Get','Del']                            
                             },
               'GetATLASInfo' : {'metricDescription': "Get the SRM full endpoints, st and LFC",
                             'cmdLineOptions'   : ['file='],
                             'cmdLineOptionsReq' : [],
                             'metricChildren'   : [],
                             'critical'         : 'N'
                             },

               'GetLHCbInfo' : {'metricDescription': "Get the SRM full endpoints from the DIRAC Configuration System",
                                 'cmdLineOptions'   : ['file='],
                                 'cmdLineOptionsReq' : [],
                                 'metricChildren'   : [],
                                 'critical'         : 'N'
                             },

               'GetPFNFromTFC' : {'metricDescription': "Get full SRM endpoints and space tokens from PhEDEx DataService TFC module.",
                             'cmdLineOptions'   : ['lfn='],
                             'cmdLineOptionsReq' : [],
                             'metricChildren'   : ['VOLsDir','VOPut','VOLs','VOGetTURLs','VOGet','VODel'],
                             'critical'         :'N'
                             },
               'LsDir'    : {'metricDescription': "List content of VO's top level space area(s) in SRM.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'Y',
                             'statusMsgs'       : {'OK'      :'OK: Storage Path directory was listed successfully.',
                                                   'WARNING' :'WARNING: Problems listing Storage Path directory.'  ,
                                                   'CRITICAL':'CRITICAL: Problems listing Storage Path directory.' ,
                                                   'UNKNOWN' :'UNKNOWN: Problems listing Storage Path directory.'}
                             },
               'VOLsDir'    : {'metricDescription': "List content of VO's top level space area(s) in SRM.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'N',
                             'statusMsgs'       : {'OK'      :'OK: Storage Path directory was listed successfully.',
                                                   'WARNING' :'WARNING: Problems listing Storage Path directory.'  ,
                                                   'CRITICAL':'CRITICAL: Problems listing Storage Path directory.' ,
                                                   'UNKNOWN' :'UNKNOWN: Problems listing Storage Path directory.'}
                             },
               'Put'      : {'metricDescription': "Copy a local file to the SRM into default space area(s).",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : ['Ls','GetTURLs','Get','Del']
                             },
               'VOPut'      : {'metricDescription': "Copy a local file to the SRM into default space area(s).",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : ['VOLs','VOGetTURLs','VOGet','VODel'],
                             'critical'         :'N'
                             },
               'Ls'       : {'metricDescription': "List (previously copied) file(s) on the SRM.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'Y',
                             'statusMsgs'       : {'OK'      :'OK: File(s) was listed successfully.',
                                                   'WARNING' :'WARNING: Problems listing file(s).'  ,
                                                   'CRITICAL':'CRITICAL: Problems listing file(s).' ,
                                                   'UNKNOWN' :'UNKNOWN: Problems listing file(s).'}                                 
                             },
               'VOLs'       : {'metricDescription': "List (previously copied) file(s) on the SRM.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'N',
                             'statusMsgs'       : {'OK'      :'OK: File(s) was listed successfully.',
                                                   'WARNING' :'WARNING: Problems listing file(s).'  ,
                                                   'CRITICAL':'CRITICAL: Problems listing file(s).' ,
                                                   'UNKNOWN' :'UNKNOWN: Problems listing file(s).'}                                 
                             },
               'GetTURLs' : {'metricDescription': "Get Transport URLs for the file copied to storage.",
                             'cmdLineOptions'   : ['se-timeout=',
                                                   'ldap-uri=',
                                                   'ldap-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'Y'
                             },
               'VOGetTURLs' : {'metricDescription': "Get Transport URLs for the file copied to storage.",
                             'cmdLineOptions'   : ['se-timeout=',
                                                   'ldap-uri=',
                                                   'ldap-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'N'
                             },
               'Get'      : {'metricDescription': "Copy given remote file(s) from SRM to a local file.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'Y'
                             },
               'VOGet'      : {'metricDescription': "Copy given remote file(s) from SRM to a local file.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'N'
                             },
               'Del'      : {'metricDescription': "Delete given file(s) from SRM.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'Y'
                             },
               'VODel'      : {'metricDescription': "Delete given file(s) from SRM.",
                             'cmdLineOptions'   : ['se-timeout='],
                             'cmdLineOptionsReq' : [],                             
                             'metricChildren'   : [],
                             'critical'         : 'N'
                             },
               'All'      : {'metricDescription': "Run all metrics.",
                             'cmdLineOptions'   : ['srmv='],
                             'cmdLineOptionsReq' : [],
                             'metricsOrder'     : ['GetSURLs','LsDir','Put','Ls','GetTURLs','Get','Del']
                             },
               'AllCMS'      : {'metricDescription': "Run all CMS metrics.",
                             'cmdLineOptions'   : [],
                             'cmdLineOptionsReq' : [],
                             'metricsOrder'     : ['GetPFNFromTFC','VOLsDir','VOPut','VOLs','VOGetTURLs','VOGet','VODel']
                             },
               'AllATLAS'      : {'metricDescription': "Run all ATLAS metrics.",
                             'cmdLineOptions'   : [],
                             'cmdLineOptionsReq' : [],
                             'metricsOrder'     : ['GetATLASInfo','VOLsDir','VOPut','VOLs','VOGet','VODel']
                             },
               'AllLHCb'      : {'metricDescription': "Run all LHCb non DIRAC specific  metrics.",
                             'cmdLineOptions'   : [],
                             'cmdLineOptionsReq' : [],
                             'metricsOrder'     : ['GetLHCbInfo','VOLsDir','VOPut','VOLs','VOGet','VODel']
                             },

               }        


    def __init__(self, tuples):
        
        probe.MetricGatherer.__init__(self, tuples, 'SRM')
       
        self.usage="""    Metrics specific options:

--srmv <1|2>           (Default: %s)

%s
--ldap-uri <URI>       Format [ldap://]hostname[:port[/]] 
                       (Default: %s)
--ldap-timeout <sec>   (Default: %i)   
    
%s
--se-timeout <sec>     (Default: %i)

!!! NOT IMPLEMENTED YET !!!
--sapath <SAPath,...>  Storage Area Path to be tested on SRM. Comma separated 
                       list of Storage Paths to be tested.

"""%(self.svcVer,
     self.ns+'.SRM-{GetSURLs,GetTURLs}',
     self._ldap_url,
     self._timeouts['ldap_timelimit'],
     self.ns+'.SRM-{LsDir,Put,Ls,GetTURLs,Get,Del}',
     self._timeouts['srm_connect'])
     
        # TODO: move to super class
        # Need to be parametrized from CLI at runtime
        self.childTimeout = 120 # timeout

        # initiate metrics description
        self.set_metrics(self._metrics)

        # parse command line parameters
        self.parse_cmd_args(tuples)

        # working directory for metrics
        self.make_workdir()

        # LDAP
        self._ldap_base = "o=grid"
        self._ldap_fileEndptSAPath = self.workdir_metric+"/EndpointAndPath"
        
        # files and patterns
        self._fileTest       = self.workdir_metric+'/testFile.txt'
        self._fileTestIn     = self.workdir_metric+'/testFileIn.txt'
        self._fileFilesOnSRM = self.workdir_metric+'/FilesOnSRM.txt'
        self._fileSRMPattern = 'testfile-put-%s-%s-%s.txt' # spacetoken, time, uuid

        # Dictionary of extra SRM info for VOs, and file to save current version and history of dictionary
        curhour=datetime.datetime.now().hour
        self._fileHistoryVoInfoDictionary = self.workdir_metric+"/VOInfoDictionary_%s"%curhour
        self._fileVoInfoDictionary = self.workdir_metric+"/VOInfoDictionary"
        #Read dictionary from current cache
        try:
            #Clean up stale current cache entries (older than 3 days) 
            try:
                modtime=os.path.getmtime(self._fileVoInfoDictionary)
                if (time.time()-modtime>3*86400):
                    self.printd('Stale VO Info cache file, deleting')
                    os.remove(self._fileVoInfoDictionary)
            except OSError:
                self.printd('VO Info cache file not found')
            self._voInfoDictionary = self.readVoInfoDictionary(self._fileVoInfoDictionary)
            self.printd('Loading VO Info dictionary from cache')
        except IOError:
            self._voInfoDictionary = {}
            self.printd('No cached VO Info dictionary; creating empty dictionary')
        except KeyError:
            self._voInfoDictionary = {}
            os.remove(self._fileVoInfoDictionary)
            self.printd('Cannot read cached VO Info dictionary; cleaning cache file and creating empty dictionary')
            
        # lcg_util and GFAL versions
        self.lcg_util_gfal_ver = gridutils.get_lcg_util_gfal_ver()


    def parse_args(self, opts):

        for o,v in opts:
            if o in ('--srmv'):
                if v in self.svcVers:
                    self.svcVer = str(v)
                else:
                    errstr = '--srmv must be one of '+\
                        ', '.join([x for x in self.svcVers])+'. '+v+' given.'
                    raise getopt.GetoptError(errstr)
            elif o in ('--ldap-uri'):
                [host, port] = samutils.parse_uri(v) 
                if port == None or port == '':
                    port = '2170'
                self._ldap_url = 'ldap://'+host+':'+port
                os.environ['LCG_GFAL_INFOSYS'] = host+':'+port
            elif o in ('--ldap-timeout'):
                self._timeouts['ldap_timelimit'] = int(v)
            elif o in ('--se-timeout'):
                self._timeouts['srm_connect'] = int(v)
    
    def __query_bdii(self, ldap_filter, ldap_attrlist, ldap_url=''):
        'Local wrapper for gridutils.query_bdii()'
        
        ldap_url = ldap_url or self._ldap_url
        try:
            tl = self._timeouts['ldap_timelimit']
        except KeyError:
            tl = None
        self.printd('Query BDII.')
        self.printd('''Parameters:
 ldap_url: %s
 ldap_timelimit: %i
 ldap_filter: %s
 ldap_attrlist: %s'''% (ldap_url, tl, ldap_filter, ldap_attrlist))
        self.print_time()
        self.printd('Querying BDII %s' % ldap_url)
        rc, qres = gridutils.query_bdii(ldap_filter, ldap_attrlist, 
                                    ldap_url=ldap_url, 
                                    ldap_timelimit=tl)
        self.print_time()
        return rc, qres

    def sig_term(self, sig, stack):
        self.chldproc.kill(signal.SIGKILL)

    def saveVoInfoDictionary(self,filename):
        fp = open(filename, "w")
        pickle.dump(self._voInfoDictionary,fp)
        fp.close()
                
    def readVoInfoDictionary(self,filename):
        fp = open(filename, "r")
        voInfoDict=pickle.load(fp)
        fp.close()
        return voInfoDict

    def weightEndpointCriticality(self,VOtest):
        DetailedMsg=''
        CriticalResult=[]
        for srmendpt in self._voInfoDictionary.keys():
          try:
            try:
              criticality=self._voInfoDictionary[srmendpt]['criticality']
            except KeyError:
              criticality=1
            if criticality==1:
              CriticalResult.append(self._voInfoDictionary[srmendpt][VOtest][0])
            #DetailedMsg = DetailedMsg + str(self._voInfoDictionary[srmendpt])
            DetailedMsg = DetailedMsg + \
			str(self._voInfoDictionary[srmendpt]['space_token']) +\
               " critical= "+ str(criticality) +\
	       " "+ str(self._voInfoDictionary[srmendpt][VOtest][1]) +\
               " file= " + str(self._voInfoDictionary[srmendpt]['fn'])+\
			"\n"
          except IndexError:
            return ('UNKNOWN', 'No SRM endpoints found in internal dictionary')
          except KeyError:
            return ('UNKNOWN', 'No test results found in internal dictionary for SRM endpoint')
        #print " GLOBAL result     \n \n \n \n \n "
        ## oredering criticality
        if 'CRITICAL' in CriticalResult:                # it's enough one CRIT  
          return ('CRITICAL' ,str(DetailedMsg))
        if 'WARNING' in CriticalResult:         #
          return ('WARNING' ,str(DetailedMsg))
        if 'UNKNOWN' in CriticalResult:                 #
          return ('UNKNOWN' ,str(DetailedMsg))
        return ('OK' ,str(DetailedMsg)) # all OK

    def metricAllCMS(self):
        return self.metricAll('AllCMS')

    def metricAllATLAS(self):
        return self.metricAll('AllATLAS')

    def metricAllLHCb(self):
        return self.metricAll('AllLHCb')

    def metricGetSURLs(self):
        """Get full SRM endpoint(s) and storage areas from BDII.
        """
        ldap_filter = "(|(&(GlueChunkKey=GlueSEUniqueID=%s)(|(GlueSAAccessControlBaseRule=%s)(GlueSAAccessControlBaseRule=VO:%s)))(&(GlueChunkKey=GlueSEUniqueID=%s)(|(GlueVOInfoAccessControlBaseRule=%s)(GlueVOInfoAccessControlBaseRule=VO:%s))) (&(GlueServiceUniqueID=*://%s*)(GlueServiceVersion=%s.*)(GlueServiceType=srm*)))" % (
                                    self.hostName,self.voName,self.voName,
                                    self.hostName,self.voName,self.voName,
                                    self.hostName,self.svcVer)
        ldap_attrlist = ['GlueServiceEndpoint', 'GlueSAPath', 'GlueVOInfoPath']

        rc, qres = self.__query_bdii(ldap_filter, ldap_attrlist, 
                                     self._ldap_url)
        if not rc:
            if qres[0] == 0: # empty set
                sts = 'CRITICAL'
            else: # all other problems
                sts = 'UNKNOWN'
            self.printd(qres[2])
            return (sts, qres[1])

        res = {}
        for k in ldap_attrlist: res[k] = []

        for entry in qres:
            for attr in res.keys():
                try:
                    for val in entry[1][attr]:
                        if val not in res[attr]:
                            res[attr].append(val)
                except KeyError: pass
        
        # GlueServiceEndpoint is not published
        k = 'GlueServiceEndpoint'
        if not res[k]:
            return ('CRITICAL',
                    "%s is not published for %s in %s" % \
                    (k, self.hostName, self._ldap_url))
        elif len(res[k]) > 1:
            return ('CRITICAL',
                    "More than one SRMv"+self.svcVer+" "+\
                    k+" is published for "+self.hostName+": "+', '.join(res[k]))
        else:
            endpoint = res[k][0]

        self.printd('GlueServiceEndpoint: %s' % endpoint)

        # GlueVOInfoPath takes precedence
        # Ref:  "Usage of Glue Schema v1.3 for WLCG Installed Capacity 
        #        information" v 1.9, Date: 03/02/2009
        if res['GlueVOInfoPath']:
            storpaths = res['GlueVOInfoPath']
            self.printd('GlueVOInfoPath: %s' % ', '.join(storpaths))
        elif res['GlueSAPath']:
            storpaths = res['GlueSAPath']
            self.printd('GlueSAPath: %s' % ', '.join(storpaths))
        else:
            # GlueSAPath or GlueVOInfoPath is not published
            return ('CRITICAL', 
                    "GlueVOInfoPath or GlueSAPath not published for %s in %s" % \
                    (res['GlueServiceEndpoint'][0], self._ldap_url))
        
        eps = [ endpoint.replace('httpg','srm',1)+'?SFN='+sp+"\n" for sp in storpaths]
        self.printd('SRM endpoint(s) to test:')
        self.printd('\n'.join(eps).strip('\n'))
        
        self.printd('Saving endpoints to %s' % self._ldap_fileEndptSAPath, v=2)
        try:
            fp = open(self._ldap_fileEndptSAPath, "w")
            for ep in eps:
                fp.write(ep)
            fp.close()
        except IOError, e:
            try: 
                os.unlink(self._ldap_fileEndptSAPath)
            except: pass
            return ('UNKNOWN', 'IOError: %s' % str(e))

        return ('OK', "Got SRM endpoint(s) and Storage Path(s) from BDII")

    def metricGetPFNFromTFC(self,testLFN="/store/unmerged/SAM/testSRM"):
        """Get full SRM endpoint(s) and storage areas from PhEDEx DataService.
        """

        #URLs for PhEDEx DataService for lfn2pfn

        tfcURL="http://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?node="
        pfnMatchURL="&lfn="
        pfnProtocolOption = "&protocol=srmv2"
        destinationOption = "&destination="
        custodialOption = "&custodial="

        #Path of text file with list of SRM endpoints
        seMapFileURL="http://cern.ch/magini/phedex-v2-endpoints.txt"
        nodeName = self.hostName

        # LFN path for file to test transfers
        self.printd('The LFN used for testing will be in: '+testLFN)
        
        try:
            self.printd('Retrieving list of endpoints to test at: %s' % seMapFileURL)
            siteNameFile=urllib2.urlopen(seMapFileURL)
        except urllib2.URLError:
            self.printd("WARNING: unable to open URL with SRM list")
            return('UNKNOWN',"Unable to open URL with SRM list")

        outputList={}
        for siteLine in siteNameFile.readlines():
            endpointName=siteLine.split()[0]
            siteName=siteLine.split()[1]
        
            if endpointName == nodeName:
                
                self.printd(nodeName+" is listed as SRM for Site "+siteName)
                
                # Contact web service to get PFN and spacetoken for non-custodial transfers
                endpointLFN = "/SAM-%s" % endpointName

                # Testing only non-custodial area - don't want to clutter custodial area with small files at T1s
                custodiality = "n"
                    
                pfnUrl = tfcURL+siteName+pfnMatchURL+testLFN+endpointLFN+pfnProtocolOption+destinationOption+siteName+custodialOption+custodiality
                self.printd("Setting custodiality flag="+custodiality)
                self.printd("Contacting webservice to perform LFN-to-PFN matching at URL:")
                self.printd(pfnUrl)

                try:
                    pfnFile=urllib2.urlopen(pfnUrl)
                except urllib2.URLError:
                    self.printd('WARNING: Unable to open PhEDEx DataService lfn2pfn URL to perform LFN-to-PFN matching for Site %s' % siteName)
                    continue

                pfnJSON = simplejson.load(pfnFile)
                
                try:
                    pfn = (((pfnJSON[u'phedex'])[u'mapping'])[0])[u'pfn']
                    spacetoken = (((pfnJSON[u'phedex'])[u'mapping'])[0])[u'space_token']
                except KeyError:
                    try:
                        errormsg = pfnJSON[u'error']
                    except KeyError:
                        self.printd('WARNING: Unknown error from PhEDEx DataService')
                        continue
                    self.printd("Error from PhEDEx DataService: ")
                    self.printd(errormsg)
                    self.printd("Possibly the site is not running a FileExport agent for the Prod instance of PhEDEx")
                    continue
                
                if pfn == None:
                    self.printd("ERROR: LFN did not match to any PFN - probably the TFC does not contain any rule for the srmv2 protocol.")
                    continue
                
                self.printd("LFN was matched to PFN "+pfn)
                if spacetoken:
                    spacetokendesc=spacetoken
                    self.printd("In space token "+spacetoken+" for custodiality="+custodiality)
                else:
                    spacetokendesc="nospacetoken"
                    self.printd("No space token defined for custodiality="+custodiality)

                if re.compile("^srm://.+srm/managerv2\?SFN=.+$").match(pfn) or re.compile("^srm://.+srm/v2/server\?SFN=.+$").match(pfn):
                    pfntonode=re.sub(":.+$","",re.sub("^srm://","",pfn))
                    if pfntonode!=nodeName :
                        self.printd("WARNING: the resulting PFN matches to SRM "+pfntonode+" instead of SRM "+nodeName)
                        continue
                    else:
                        fn = self._fileSRMPattern % (spacetokendesc,str(int(time.time())), 
                                                     samutils.uuidstr())
                        outputList[pfn]={'fn': fn, 'space_token': spacetoken, 'userspace' : testLFN}
                else:
                    self.printd("WARNING: Invalid matching to srmv2 protocol")
                    self.printd("Note: this test currently supports only PFNs in the known srmv2 full endpoint formats:")
                    self.printd("srm://hostname:port/srm/managerv2?SFN=sitefilename")
                    self.printd("or")
                    self.printd("srm://hostname:port/srm/v2/server?SFN=sitefilename")

        # Extract a random PFN from the dictionary of PFN matches. It will be used for testing, other PFN matches will be ignored
        # Print warning if not all PFN matches are the same.
        try:
            outputPfn=outputList.popitem()
        except KeyError:
            self.printd("WARNING: "+nodeName+" not found in SRM list")
            return('UNKNOWN',"WARNING: "+nodeName+" not found in SRM list")

        for otherOutputPfns in outputList:
            if otherOutputPfns != outputPfn:
                self.printd("WARNING: PFN matching was not the same on all PhEDEx nodes associated to SRM "+nodeName)

        self.printd("The PFN path used for testing will be:")
        self.printd(str(outputPfn))
        
        self._voInfoDictionary[outputPfn[0]]=outputPfn[1]

        self.printd('Saving endpoints to %s' % self._fileVoInfoDictionary, v=2)
        self.printd('Test results will be saved to %s' % self._fileHistoryVoInfoDictionary, v=2)

        try:
            self.saveVoInfoDictionary(self._fileVoInfoDictionary)
        except IOError:
            self.printd('Error saving VO Info Dictionary to file %s' % self._fileVoInfoDictionary)
        try:
            self.saveVoInfoDictionary(self._fileHistoryVoInfoDictionary)
        except IOError:
            self.printd('Error saving VO Info Dictionary history to file %s' % self._fileHistoryVoInfoDictionary)

        return ('OK', "Got PFN and Space Token from PhEDEx DataService")

    def metricGetATLASInfo(self):
        """Get full SRM endpoint(s) and storage areas from ToACache.
        """
        agis_file="/afs/cern.ch/user/d/digirola/public/nagios_atlas/project/src/SRM/org.atlas/src/ToA_srm2_list"         
        agis=open(agis_file, 'r')
        agis_endpoint_info=[]
        for entry in agis:
          if entry.find(self.hostName) != -1:
            #print entry
            agis_endpoint_info.append(entry[:-1])
            spacetokendesc=entry.split()[1]
            fn = self._fileSRMPattern % (spacetokendesc,str(int(time.time())),
                                                     samutils.uuidstr())
            if spacetokendesc in ('ATLASDATADISK','ATLASMCDISK','ATLASGROUPDISK'):
              criticality=1
            else: 
              criticality=0
            #endpoint (spacetoken) criticality
            agis_endpoint_details = {
              'fn': fn, 
              'space_token': spacetokendesc,
              'criticality': criticality,
            }
            self._voInfoDictionary[entry.split()[0]+'SAM']=agis_endpoint_details

        #print agis_endpoint_info
        self.printd(str(agis_endpoint_info))
        self.printd(str(self._voInfoDictionary))
        try:
          fp = open(self._ldap_fileEndptSAPath, "w")
          for info in agis_endpoint_info:
            ep=info.split()[0]+'\n'   
            fp.write(ep)
          fp.close()
        except IOError, e:
          try:
            os.unlink(self._ldap_fileEndptSAPath)
          except: pass
          return ('UNKNOWN', 'IOError: %s' % str(e))

 
        #print self._ldap_fileEndptSAPath
        return ('OK',"Endpoint informations found in ToA cached file ")


    def metricGetLHCbInfo(self):
        """Get full SRM endpoint(s) and storage areas from ToACache.
        """
        dirac_file="/afs/cern.ch/user/r/roiser/public/inproduction/ToA_srm2_list"
        #dirac_file="/afs/cern.ch/user/s/santinel/public/www/ATP/ToA_srm2_list"
        dirac=open(dirac_file, 'r')
        dirac_endpoint_info=[]
        for entry in dirac:
          if entry.find(self.hostName) != -1:
            #print entry
            dirac_endpoint_info.append(entry[:-1])
            spacetokendesc=entry.split()[1]
            fn = self._fileSRMPattern % (spacetokendesc,str(int(time.time())),
                                                     samutils.uuidstr())
            if spacetokendesc in ('LHCb_USER','LHCb_M-DST','LHCb_RAW'):
              criticality=1
            else:
              criticality=0
            #endpoint (spacetoken) criticality
            dirac_endpoint_details = {
              'fn': fn, 
              'space_token': spacetokendesc,
              'criticality': criticality,
            }
            self._voInfoDictionary[entry.split()[0]+'/SAM']=dirac_endpoint_details

        #print dirac_endpoint_info
        self.printd(str(dirac_endpoint_info))
        self.printd(str(self._voInfoDictionary))
        try:
          fp = open(self._ldap_fileEndptSAPath, "w")
          for info in dirac_endpoint_info:
            ep=info.split()[0]+'\n'
            fp.write(ep)
          fp.close()
        except IOError, e:
          try:
            os.unlink(self._ldap_fileEndptSAPath)
          except: pass
          return ('UNKNOWN', 'IOError: %s' % str(e))

        #print self._ldap_fileEndptSAPath
        return ('OK',"Endpoint informations found in ToA cached file ")


    def metricLsDir(self):
        "List content of VO's top level space area(s) in SRM using gfal_ls()."

        status = 'OK'
        summary = ''
        self.printd(self.lcg_util_gfal_ver)
        
        srms = []
        try:
            for srm in open(self._ldap_fileEndptSAPath, 'r'):
                srms.append(srm.rstrip('\n'))
            if not srms:
                return ('UNKNOWN', 'No SRM endpoints found in %s' % 
                                    self._ldap_fileEndptSAPath)
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')

        signal.signal(signal.SIGALRM, self.sig_term)
        signal.alarm(self.childTimeout)
        
        req = {'surls'          : srms,
               'defaultsetype'  : 'srmv'+self.svcVer,
               'setype'         : 'srmv'+self.svcVer,
               'timeout'        : self._timeouts['srm_connect'],
               'srmv2_lslevels' : 0,
               'no_bdii_check'  : 1
               }
        self.printd('Using gfal_ls().') 
        self.printd('Parameters:\n%s' % '\n'.join(
                        ['  %s: %s' % (x,str(y)) for x,y in req.items()]))
        errmsg = ''
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_init(req)
        except MemoryError, e:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            summary = 'error initialising GFAL: %s' % str(e)
            self.printd('ERROR: %s' % summary)
            return ('UNKNOWN', summary)
        else:
            if rc != 0:
                summary = 'problem initialising GFAL: %s' % errmsg
                self.printd('ERROR: %s' % summary)
                return ('UNKNOWN', summary)

        self.print_time()
        self.printd('Listing storage url(s).')
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_ls(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            return ('UNKNOWN', 'problem invoking gfal_ls(): %s' % errmsg)
        else:
            self.print_time()
            if rc != 0:
                try: gfal.gfal_internal_free(gfalobj)
                except: pass
                em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                er = em.match(errmsg)
                summary = 'problem listing Storage Path(s).'
                if er:
                    if status != 'CRITICAL':
                        status = er[0][2]
                    summary += ' [ErrDB:%s]' % str(er)
                else:
                    status = 'CRITICAL'
                self.printd('ERROR: %s' % errmsg)
                return (status, summary)

        try:
            (rc, gfalobj, gfalstatuses) = gfal.gfal_get_results(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass            
            raise
        else:
            summary = ''
            for st in gfalstatuses:
                summary += 'Storage Path[%s]' % st['surl']
                self.printd('Storage Path[%s]' % st['surl'], cr=False)
                if st['status'] != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(st['explanation'])
                    if er:
                        if status != 'CRITICAL':
                            status = er[0][2]
                        summary += '-%s [ErrDB:%s];' % (status.lower(), str(er))
                    else:
                        status = 'CRITICAL'
                        summary += '-%s;' % status.lower()
                    self.printd('-%s;\nERROR: %s\n' % (status.lower(), st['explanation']))
                else:
                    summary += '-ok;'
                    self.printd('-ok;')

        try: gfal.gfal_internal_free(gfalobj)
        except: pass

        return (status, summary)

    def metricVOLsDir(self):
        "List content of VO's top level space area(s) in SRM using gfal_ls()."

        status = 'OK'
        summary = ''
        self.printd(self.lcg_util_gfal_ver)
        
        srms = self._voInfoDictionary.keys()

        signal.signal(signal.SIGALRM, self.sig_term)
        signal.alarm(self.childTimeout)
        
        req = {'surls'          : srms,
               'defaultsetype'  : 'srmv'+self.svcVer,
               'setype'         : 'srmv'+self.svcVer,
               'timeout'        : self._timeouts['srm_connect'],
               'srmv2_lslevels' : 0,
               'no_bdii_check'  : 1
               }
        self.printd('Using gfal_ls().') 
        self.printd('Parameters:\n%s' % '\n'.join(
                        ['  %s: %s' % (x,str(y)) for x,y in req.items()]))
        errmsg = ''
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_init(req)
        except MemoryError, e:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            summary = 'error initialising GFAL: %s' % str(e)
            self.printd('ERROR: %s' % summary)
            return ('UNKNOWN', summary)
        else:
            if rc != 0:
                summary = 'problem initialising GFAL: %s' % errmsg
                self.printd('ERROR: %s' % summary)
                return ('UNKNOWN', summary)

        self.print_time()
        self.printd('Listing storage url(s).')
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_ls(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            return ('UNKNOWN', 'problem invoking gfal_ls(): %s' % errmsg)
        else:
            self.print_time()
            if rc != 0:
                try: gfal.gfal_internal_free(gfalobj)
                except: pass
                em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                er = em.match(errmsg)
                summary = 'problem listing Storage Path(s).'
                if er:
                    if status != 'CRITICAL':
                        status = er[0][2]
                    summary += ' [ErrDB:%s]' % str(er)
                else:
                    status = 'CRITICAL'
                self.printd('ERROR: %s' % errmsg)
                return (status, summary)

        try:
            (rc, gfalobj, gfalstatuses) = gfal.gfal_get_results(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass            
            raise
        else:
            summary = ''
            for st in gfalstatuses:
                summary += 'Storage Path[%s]' % st['surl']
                self.printd('Storage Path[%s]' % st['surl'], cr=False)
                if st['status'] != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(st['explanation'])
                    if er:
                        if status != 'CRITICAL':
                            status = er[0][2]
                        summary += '-%s [ErrDB:%s];' % (status.lower(), str(er))
                    else:
                        status = 'CRITICAL'
                        summary += '-%s;' % status.lower()
                    self.printd('-%s;\nERROR: %s\n' % (status.lower(), st['explanation']))
                else:
                    summary += '-ok;'
                    self.printd('-ok;')

        try: gfal.gfal_internal_free(gfalobj)
        except: pass

        return (status, summary)
    
    def metricLsDir2(self):
        "List content of VO's top level space area(s) in SRM using lcg-ls."

        status = 'OK'
        summary = detmsg = ''
        
        try:
            srms = []
            for srm in open(self._ldap_fileEndptSAPath, 'r'):
                srms.append(srm.rstrip('\n'))
        except IOError, e:
            try: 
                os.unlink(self._ldap_fileEndptSAPath)
            except: pass
            return ('UNKNOWN', "IOError: %s" % str(e))

        signal.signal(signal.SIGALRM, self.sig_term)
        signal.alarm(self.childTimeout)
        
        for srm in srms:
            cmd = "lcg-ls %s -t "+str(self._timeouts['srm_connect'])+" -b --vo "+self.voName+\
                " -T srmv"+self.svcVer+" -l -d "+srm
            (status, summary, detmsg) = self.run_cmd(cmd)

        return (status, summary, detmsg) 

    def metricVOPut(self):
        "Copy a local file to the SRM into space area(s) defined by VO."
        
        self.printd(self.lcg_util_gfal_ver)
        
        # generate source file
        try:
            src_file = self._fileTest
            fp = open(src_file, "w")
            for s in "1234567890": fp.write(s+'\n')
            fp.close()
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')

        for srmendpt in self._voInfoDictionary.keys():
                        
            self.printd('VOPut: Copy file using lcg_cp3().')
            # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
            # SRM types: string to integer mapping
            # TYPE_NONE  -> 0
            # TYPE_SRM   -> 1
            # TYPE_SRMv2 -> 2
            # TYPE_SE    -> 3
            defaulttype = int(self.svcVer)
            srctype     = 0
            dsttype     = defaulttype
            nobdii      = 1
            vo          = self.voName
            nbstreams   = 1
            conf_file   = ''
            insecure    = 0
            verbose     = 0 # if self.verbosity > 0: verbose = 1 # when API is fixed
            timeout     = self._timeouts['srm_connect']
            src_spacetokendesc  = ''
            try:
                dest_spacetokendesc = (self._voInfoDictionary[srmendpt])['space_token']
            except KeyError:
                dest_spacetokendesc = ''
                
            self.printd('''Parameters:
 defaulttype: %i
 srctype: %i
 dsttype: %i
 nobdi: %i
 vo: %s
 nbstreams: %i
 conf_file: %s
 insecure: %i
 verbose: %i
 timeout: %i
 src_spacetokendesc: %s
 dest_spacetokendesc: %s''' % (defaulttype, srctype,
                               dsttype, nobdii, vo, nbstreams, conf_file or '-',
                               insecure, verbose, timeout, 
                               src_spacetokendesc or '-', dest_spacetokendesc or '-'))
            
            errmsg = ''
            stMsg = 'File was%s copied to SRM.' 
            start_transfer = datetime.datetime.now()
            #self.print_time()
            self.printd('StartTime of the transfer: %s' % str(start_transfer)) 
            dest_filename=(self._voInfoDictionary[srmendpt])['fn']
            dest_file=srmendpt+'/'+dest_filename
            
            self.printd('Destination: %s' % dest_file)
            try:
                rc, errmsg = \
                    lcg_util.lcg_cp3(src_file, dest_file, defaulttype, srctype,
                                     dsttype, nobdii, vo, nbstreams, conf_file,
                                     insecure, verbose, timeout, 
                                     src_spacetokendesc, dest_spacetokendesc)
            except AttributeError, e:
                status = 'UNKNOWN'
                summary = stMsg % ' NOT'
                self.printd('ERROR: %s %s' % (str(e), sys.exc_info()[0]))
            else:
                if rc != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                        summary = stMsg % (' NOT')+' [ErrDB:%s]' % str(er)
                    else:
                        status = 'CRITICAL'
                        summary = stMsg % ' NOT'   #ADD HERE the full error msg errmsg
                    self.printd('ERROR: %s' % errmsg)
                else:
                    status = 'OK'
                    total_transfer = datetime.datetime.now()-start_transfer
                    self.printd('Transfer Duration: %s' % str(total_transfer))
                    summary = stMsg % ''+ " Transfer time: "+str(total_transfer)
            #self.print_time()
            (self._voInfoDictionary[srmendpt])['putResult']=(status,summary)

        try:
            self.saveVoInfoDictionary(self._fileHistoryVoInfoDictionary)
        except IOError:
            self.printd('Error saving VO Info Dictionary to file %s' % self._fileHistoryVoInfoDictionary)

        #EXTRACT ARIBITRARY ITEM FROM THE DICTIONARY TO RETURN RESULTS
        #REPLACE WITH WEIGHTED CALCULATION BASED ON CRITICALITY OF PATHS/ENDPOINTS!!!
        # weightedResult will return a tuple with nagiosexitcode and detailed output
        weightedResult=self.weightEndpointCriticality('putResult')
        return weightedResult
         ## what if no srmendpt?
        
    def metricPut(self):
        "Copy a local file to the SRM into default space area(s)."
        
        self.printd(self.lcg_util_gfal_ver)
        
        # generate source file
        try:
            src_file = self._fileTest
            fp = open(src_file, "w")
            for s in "1234567890": fp.write(s+'\n')
            fp.close()
            
            # multiple 'SAPath's are possible
            dest_files = []
            fn = self._fileSRMPattern % (str(int(time.time())), 
                                         samutils.uuidstr())
            for srmendpt in open(self._ldap_fileEndptSAPath):
                dest_files.append(srmendpt.rstrip('\n')+'/'+fn)
            if not dest_files:
                return ('UNKNOWN', 'No SRM endpoints found in %s' % 
                                    self._ldap_fileEndptSAPath)
    
            fp = open(self._fileFilesOnSRM, "w")
            for dfile in dest_files:
                fp.write(dfile+'\n')
            fp.close()
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')


        self.printd('Copy file using lcg_cp3().')
        # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
        # SRM types: string to integer mapping
        # TYPE_NONE  -> 0
        # TYPE_SRM   -> 1
        # TYPE_SRMv2 -> 2
        # TYPE_SE    -> 3
        defaulttype = int(self.svcVer)
        srctype     = 0
        dsttype     = defaulttype
        nobdii      = 1
        vo          = self.voName
        nbstreams   = 1
        conf_file   = ''
        insecure    = 0
        verbose     = 0 # if self.verbosity > 0: verbose = 1 # when API is fixed
        timeout     = self._timeouts['srm_connect']
        src_spacetokendesc  = ''
        dest_spacetokendesc = ''

        self.printd('''Parameters:
 defaulttype: %i
 srctype: %i
 dsttype: %i
 nobdi: %i
 vo: %s
 nbstreams: %i
 conf_file: %s
 insecure: %i
 verbose: %i
 timeout: %i
 src_spacetokendesc: %s
 dest_spacetokendesc: %s''' % (defaulttype, srctype,
                      dsttype, nobdii, vo, nbstreams, conf_file or '-',
                      insecure, verbose, timeout, 
                      src_spacetokendesc or '-', dest_spacetokendesc or '-'))

        errmsg = ''
        stMsg = 'File was%s copied to SRM.'
        for dest_file in dest_files:
            self.print_time()
            self.printd('Destination: %s' % dest_file)
            try:
                rc, errmsg = \
                    lcg_util.lcg_cp3(src_file, dest_file, defaulttype, srctype,
                                     dsttype, nobdii, vo, nbstreams, conf_file,
                                     insecure, verbose, timeout, 
                                     src_spacetokendesc, dest_spacetokendesc)
            except AttributeError, e:
                status = 'UNKNOWN'
                summary = stMsg % ' NOT'
                self.printd('ERROR: %s %s' % (str(e), sys.exc_info()[0]))
            else:
                if rc != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                        summary = stMsg % (' NOT')+' [ErrDB:%s]' % str(er)
                    else:
                        status = 'CRITICAL'
                        summary = stMsg % ' NOT'
                    self.printd('ERROR: %s' % errmsg)
                else:
                    status = 'OK'
                    summary = stMsg % ''
            self.print_time()
        return (status, summary)

    def metricLs(self):
        "List (previously copied) file(s) on the SRM."
        
        self.printd(self.lcg_util_gfal_ver)
        
        status = 'OK'
        
        srms = []
        try:
            for sfile in open(self._fileFilesOnSRM, 'r'):
                srms.append(sfile.rstrip('\n'))
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')

        signal.signal(signal.SIGALRM, self.sig_term)
        signal.alarm(self.childTimeout)
        
        req = {'surls'          : srms,
               'defaultsetype'  : 'srmv'+self.svcVer,
               'setype'         : 'srmv'+self.svcVer,
               'no_bdii_check'  : 1,
               'timeout'        : self._timeouts['srm_connect'],
               'srmv2_lslevels' : 0               
               }
        self.printd('Using gfal_ls().') 
        self.printd('Parameters:\n%s' % '\n'.join(
                        ['  %s: %s' % (x,str(y)) for x,y in req.items()]))
        errmsg = ''
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_init(req)
        except MemoryError, e:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            summary = 'error initialising GFAL: %s' % str(e)
            self.printd('ERROR: %s' % summary)
            return ('UNKNOWN', summary)
        else:
            if rc != 0:
                summary = 'problem initialising GFAL: %s' % errmsg
                self.printd('ERROR: %s' % summary)
                return ('UNKNOWN', summary)

        self.print_time()
        self.printd('Listing file(s).')
        errmsg = ''
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_ls(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            return ('UNKNOWN', 'problem invoking gfal_ls(): %s' % errmsg)
        else:
            self.print_time()
            if rc != 0:
                try: gfal.gfal_internal_free(gfalobj)
                except: pass
                em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                er = em.match(errmsg)
                summary = 'problem listing file(s).'
                if er:
                    if status != 'CRITICAL':
                        status = er[0][2]
                    summary += ' [ErrDB:%s]' % str(er)
                else:
                    status = 'CRITICAL'
                self.printd('ERROR: %s' % errmsg)
                return (status, summary)

        try:
            (rc, gfalobj, gfalstatuses) = gfal.gfal_get_results(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass            
            raise
        else:
            summary = ''
            for st in gfalstatuses:
                summary += 'listing [%s]' % st['surl']
                self.printd('listing [%s]' % st['surl'], cr=False)
                if st['status'] != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(st['explanation'])
                    if er:
                        if status != 'CRITICAL':
                            status = er[0][2]
                        summary += '-%s [ErrDB:%s];' % (status.lower(), str(er))
                    else:
                        status = 'CRITICAL'
                        summary += '-%s;' % status.lower()
                    self.printd('-%s;\nERROR: %s\n' % (status.lower(), st['explanation']))
                else:
                    summary += '-ok;'
                    self.printd('-ok;')

        try: gfal.gfal_internal_free(gfalobj)
        except: pass

        return (status, summary)

    def metricVOLs(self):
        "List (previously copied) file(s) on the SRM."
        
        self.printd(self.lcg_util_gfal_ver)
        
        status = 'OK'
        
        srms = []

        for srmendpt in self._voInfoDictionary.keys():
            dest_filename=(self._voInfoDictionary[srmendpt])['fn']
            dest_file=srmendpt+'/'+dest_filename
            srms.append(dest_file)

        self.print_time()
            
        signal.signal(signal.SIGALRM, self.sig_term)
        signal.alarm(self.childTimeout)
        
        req = {'surls'          : srms,
               'defaultsetype'  : 'srmv'+self.svcVer,
               'setype'         : 'srmv'+self.svcVer,
               'no_bdii_check'  : 1,
               'timeout'        : self._timeouts['srm_connect'],
               'srmv2_lslevels' : 0               
               }
        self.printd('Using gfal_ls().') 
        self.printd('Parameters:\n%s' % '\n'.join(
                        ['  %s: %s' % (x,str(y)) for x,y in req.items()]))
        errmsg = ''
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_init(req)
        except MemoryError, e:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            summary = 'error initialising GFAL: %s' % str(e)
            self.printd('ERROR: %s' % summary)
            return ('UNKNOWN', summary)
        else:
            if rc != 0:
                summary = 'problem initialising GFAL: %s' % errmsg
                self.printd('ERROR: %s' % summary)
                return ('UNKNOWN', summary)

        self.print_time()
        self.printd('Listing file(s).')
        errmsg = ''
        try:
            (rc, gfalobj, errmsg) = gfal.gfal_ls(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass
            return ('UNKNOWN', 'problem invoking gfal_ls(): %s' % errmsg)
        else:
            self.print_time()
            if rc != 0:
                try: gfal.gfal_internal_free(gfalobj)
                except: pass
                em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                er = em.match(errmsg)
                summary = 'problem listing file(s).'
                if er:
                    if status != 'CRITICAL':
                        status = er[0][2]
                    summary += ' [ErrDB:%s]' % str(er)
                else:
                    status = 'CRITICAL'
                self.printd('ERROR: %s' % errmsg)
                return (status, summary)

        try:
            (rc, gfalobj, gfalstatuses) = gfal.gfal_get_results(gfalobj)
        except:
            try: gfal.gfal_internal_free(gfalobj)
            except: pass            
            raise
        else:
            summary = ''
            for st in gfalstatuses:
                summary += 'listing [%s]' % st['surl']
                self.printd('listing [%s]' % st['surl'], cr=False)
                if st['status'] != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(st['explanation'])
                    if er:
                        if status != 'CRITICAL':
                            status = er[0][2]
                        summary += '-%s [ErrDB:%s];' % (status.lower(), str(er))
                    else:
                        status = 'CRITICAL'
                        summary += '-%s;' % status.lower()
                    self.printd('-%s;\nERROR: %s\n' % (status.lower(), st['explanation']))
                else:
                    summary += '-ok;'
                    self.printd('-ok;')

        try: gfal.gfal_internal_free(gfalobj)
        except: pass

        return (status, summary)

    def metricLs2(self):
        "List (previously copied) file(s) on the SRM."

        status = 'OK'
        summary = detmsg = ''
        
        try:
            src_files = []
            for sfile in open(self._fileFilesOnSRM, 'r'):
                src_files.append(sfile.rstrip('\n'))
        except IOError, e:
            try: 
                os.unlink(self._fileFilesOnSRM)
            except: pass
            return ('UNKNOWN', "IOError: %s" % str(e))

        signal.signal(signal.SIGALRM, self.sig_term)
        signal.alarm(self.childTimeout)

        for srm in src_files:
            # TODO: use Pexpect for line-buffered output
            cmd = "lcg-ls -t "+str(self._timeouts['srm_connect'])+" %s -b --vo "+self.voName+\
                " -T srmv"+self.svcVer+" -l "+srm
            (status, summary, detmsg) = self.run_cmd(cmd)

        return(status, summary, detmsg)

    def metricGetTURLs(self):
        "Get Transport URLs for the file copied to storage."

        self.printd(self.lcg_util_gfal_ver)

        # discover transport protocols  
        ldap_filter = "(&(objectclass=GlueSEAccessProtocol)"+\
                        "(GlueChunkKey=GlueSEUniqueID=%s))" % self.hostName
        ldap_attrlist = ['GlueSEAccessProtocolType']

        rc, qres = self.__query_bdii(ldap_filter, ldap_attrlist, 
                                     self._ldap_url)
        if not rc:
            if qres[0] == 0: # empty set
                sts = 'WARNING'
            else: # all other problems
                sts = 'UNKNOWN'
            self.printd(qres[2])
            return (sts, qres[1])

        protos = []
        for e in qres:
            if e[1]['GlueSEAccessProtocolType'][0] not in protos:
                protos.append(e[1]['GlueSEAccessProtocolType'][0])
            
        if not protos:
            return ('WARNING', "No access protocol types for %s published in %s" % \
                                (self.hostName, self._ldap_url))
        
        self.printd('Discovered GlueSEAccessProtocolType: %s' % ', '.join(protos))

        src_files = []
        try:
            for sfile in open(self._fileFilesOnSRM, 'r'):
                src_files.append(sfile.rstrip('\n'))
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')

        rc = None 
        turl = None
        reqid = None
        fileid = None
        token = None
        errmsg = None
        # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
        # SRM types: string to integer mapping
        # TYPE_NONE  -> 0
        # TYPE_SRM   -> 1
        # TYPE_SRMv2 -> 2
        # TYPE_SE    -> 3
        defaulttype = int(self.svcVer)
        setype      = defaulttype
        nobdii      = 1
        timeout     = self._timeouts['srm_connect']
        spacetokendesc = None
        
        self.printd('Using lcg_gt3().')
        self.printd('''Parameters:
 defaulttype: %i
 setype: %i
 nobdii: %i
 timeout: %i
 spacetokendesc: %s''' % (defaulttype, setype, nobdii, timeout, 
                          spacetokendesc or '-'))
        
        ok = []; nok = []
        status = 'OK'
        for src_file in src_files:
            self.printd('=====\nSURL: %s\n-----' % src_file)
            for proto in protos:
                self.print_time()
                errmsg = ''
                try:
                    (rc, turl, reqid, fileid, token, errmsg) = \
                        lcg_util.lcg_gt3(src_file, defaulttype, setype, nobdii,
                                    [proto], timeout, spacetokendesc)
                    (rc, errmsg) = lcg_util.lcg_sd3(src_file, nobdii, reqid,
                                                        fileid, token, timeout)
                except Exception, e:
                    status = 'UNKNOWN'
                    self.printd('ERROR: %s\n%s' % (errmsg, str(e)))
                else:                                
                    if rc != 0:
                        if not proto in nok:
                            nok.append(proto)
                        self.printd('proto: %s - FAILED' % proto)
                        self.printd('error: %s' % errmsg)
                        em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                        er = em.match(errmsg)
                        if er:
                            status = er[0][2]
                        else:
                            status = 'CRITICAL'
                    else:
                        if not proto in ok:
                            ok.append(proto)
                        self.printd('proto: %s - OK' % proto)
                        if not samutils.to_retcode(status) > samutils.to_retcode('OK'):
                            status = 'OK'
                self.print_time()
                self.printd('-----')

        summary = 'protocols OK-[%s]' % ', '.join([x for x in ok])
        if nok: 
            summary += ', FAILED-[%s]' % ', '.join([x for x in nok])
        
        return (status, summary)

    def metricVOGetTURLs(self):
        "Get Transport URLs for the file copied to storage."

        self.printd(self.lcg_util_gfal_ver)

        for srmendpt in self._voInfoDictionary.keys():
                        
            self.print_time()
            src_filename=(self._voInfoDictionary[srmendpt])['fn']
            src_file=srmendpt+'/'+src_filename

            rc = None 
            turl = None
            reqid = None
            fileid = None
            token = None
            errmsg = None
            # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
            # SRM types: string to integer mapping
            # TYPE_NONE  -> 0
            # TYPE_SRM   -> 1
            # TYPE_SRMv2 -> 2
            # TYPE_SE    -> 3
            defaulttype = int(self.svcVer)
            setype      = defaulttype
            nobdii      = 1
            protocol    = ['gsiftp']
            timeout     = self._timeouts['srm_connect']
            spacetokendesc = None
            
            self.printd('Using lcg_gt3().')
            self.printd('''Parameters:
 defaulttype: %i
 setype: %i
 nobdii: %i
 protocol: %s
 timeout: %i
 spacetokendesc: %s''' % (defaulttype, setype, nobdii, protocol, timeout, 
                          spacetokendesc or '-'))
        
            status = 'OK'
            self.printd('=====\nSURL: %s\n-----' % src_file)
            self.print_time()
            errmsg = ''
            try:
                (rc, turl, reqid, fileid, token, errmsg) = \
                     lcg_util.lcg_gt3(src_file, defaulttype, setype, nobdii,
                                      protocol, timeout, spacetokendesc)
                (rc, errmsg) = lcg_util.lcg_sd3(src_file, nobdii, reqid,
                                                fileid, token, timeout)
            except Exception, e:
                status = 'UNKNOWN'
                self.printd('ERROR: %s\n%s' % (errmsg, str(e)))
                summary = 'protocol UNKNOWN-[%s]' % protocol
            else:                                
                if rc != 0:
                    self.printd('proto: %s - FAILED' % protocol)
                    self.printd('error: %s' % errmsg)
                    summary = 'protocol FAILED-[%s]' % protocol
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                    else:
                        status = 'CRITICAL'
                else:
                    self.printd('proto: %s - OK' % protocol)
                    self.printd('TURL: %s' % turl)
                    status = 'OK'
                    summary = 'protocol OK-[%s], TURL: %s' % (protocol,turl)
                    
            self.print_time()
            self.printd('-----')

            (self._voInfoDictionary[srmendpt])['getTURLResult']=(status,summary)

        try:
            self.saveVoInfoDictionary(self._fileHistoryVoInfoDictionary)
        except IOError:
            self.printd('Error saving VO Info Dictionary to file %s' % self._fileHistoryVoInfoDictionary)

        #EXTRACT ARIBITRARY ITEM FROM THE DICTIONARY TO RETURN RESULTS
        #REPLACE WITH WEIGHTED CALCULATION BASED ON CRITICALITY OF PATHS/ENDPOINTS!!!

        try:
            return (self._voInfoDictionary.values()[0])['getTURLResult']
        except IndexError:
            return ('UNKNOWN', 'No SRM endpoints found in internal dictionary')
        except KeyError:
            return ('UNKNOWN', 'No test results found in internal dictionary for SRM endpoint')
        

    def metricGet(self):
        "Copy given remote file(s) from SRM to a local file."

        self.printd(self.lcg_util_gfal_ver)

        # multiple 'Storage Path's are possible
        src_files = []
        try:
            for sfile in open(self._fileFilesOnSRM, 'r'):
                src_files.append(sfile.rstrip('\n'))
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')
        
        dest_file = 'file:'+self._fileTestIn

        self.printd('Get file using lcg_cp3().')
        # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
        # SRM types string to integer mapping
        # TYPE_NONE  -> 0
        # TYPE_SRM   -> 1
        # TYPE_SRMv2 -> 2
        # TYPE_SE    -> 3
        defaulttype = int(self.svcVer)
        srctype     = defaulttype
        dsttype     = 0
        nobdii      = 1
        vo          = self.voName
        nbstreams   = 1
        conf_file   = ''
        insecure    = 0
        verbose     = 0 # if self.verbosity > 0: verbose = 1 # when API is fixed
        timeout     = self._timeouts['srm_connect']
        src_spacetokendesc  = ''
        dest_spacetokendesc = ''
        
        self.printd('''Parameters:
 defaulttype: %i
 srctype: %i
 dsttype: %i
 nobdi: %i
 vo: %s
 nbstreams: %i
 conf_file: %s
 insecure: %i
 verbose: %i
 timeout: %i
 src_spacetokendesc: %s
 dest_spacetokendesc: %s''' % (defaulttype, srctype,
                      dsttype, nobdii, vo, nbstreams, conf_file or '-',
                      insecure, verbose, timeout, 
                      src_spacetokendesc or '-', dest_spacetokendesc or '-'))
        
        stMsg = 'File was%s copied from SRM.'
        for src_file in src_files:
            self.print_time()
            self.printd('Source: %s' % src_file)
            errmsg = ''
            try:
                rc, errmsg = \
                    lcg_util.lcg_cp3(src_file, dest_file, defaulttype, srctype,
                                     dsttype, nobdii, vo, nbstreams, conf_file,
                                     insecure, verbose, timeout, 
                                     src_spacetokendesc, dest_spacetokendesc);
            except Exception, e:
                status = 'UNKNOWN'
                summary = stMsg % ' NOT'
                self.printd('ERROR: %s\n%s' % (errmsg, str(e)))
            else:
                if rc != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                        summary = stMsg % (' NOT')+'[ErrDB:%s]' % str(er)
                    else:
                        status = 'CRITICAL'
                        summary = stMsg % ' NOT'
                    self.printd('ERROR: %s' % errmsg)
                else:
                    cmd = '`which diff` %s %s' % (self._fileTest, self._fileTestIn)
                    res = commands.getstatusoutput(cmd)
                    if res[0] == 0:
                        status = 'OK'
                        summary = stMsg % ('')+' Diff successful.'
                    elif res[0] == 256: # files differ
                        status = 'CRITICAL'
                        summary = stMsg % ('')+' Files differ!'
                        self.printd('diff ERROR: %s' % res[1])
                    else:
                        status = 'UNKNOWN'
                        summary = stMsg % ''+' Unknown problem when comparing files!'
                        self.printd('diff ERROR: %s' % res[1])
            self.print_time()

        return(status, summary)

    def metricVOGet(self):
        "Copy given remote file(s) from SRM to a local file."

        self.printd(self.lcg_util_gfal_ver)

        for srmendpt in self._voInfoDictionary.keys():
                        
            self.print_time()
            src_filename=(self._voInfoDictionary[srmendpt])['fn']
            src_file=srmendpt+'/'+src_filename

            dest_file = 'file:'+self._fileTestIn

            self.printd('Source: %s' % src_file)
            self.printd('Destination: %s' % dest_file)

            self.printd('Get file using lcg_cp3().')
            # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
            # SRM types string to integer mapping
            # TYPE_NONE  -> 0
            # TYPE_SRM   -> 1
            # TYPE_SRMv2 -> 2
            # TYPE_SE    -> 3
            defaulttype = int(self.svcVer)
            srctype     = defaulttype
            dsttype     = 0
            nobdii      = 1
            vo          = self.voName
            nbstreams   = 1
            conf_file   = ''
            insecure    = 0
            verbose     = 0 # if self.verbosity > 0: verbose = 1 # when API is fixed
            timeout     = self._timeouts['srm_connect']
            src_spacetokendesc  = ''
            dest_spacetokendesc = ''
        
            self.printd('''Parameters:
 defaulttype: %i
 srctype: %i
 dsttype: %i
 nobdi: %i
 vo: %s
 nbstreams: %i
 conf_file: %s
 insecure: %i
 verbose: %i
 timeout: %i
 src_spacetokendesc: %s
 dest_spacetokendesc: %s''' % (defaulttype, srctype,
                      dsttype, nobdii, vo, nbstreams, conf_file or '-',
                      insecure, verbose, timeout, 
                      src_spacetokendesc or '-', dest_spacetokendesc or '-'))
        
            stMsg = 'File was%s copied from SRM.'
            errmsg = ''
            start_transfer = datetime.datetime.now()
            #self.print_time()
            self.printd('StartTime of the transfer: %s' % str(start_transfer))
            try:
                rc, errmsg = \
                    lcg_util.lcg_cp3(src_file, dest_file, defaulttype, srctype,
                                     dsttype, nobdii, vo, nbstreams, conf_file,
                                     insecure, verbose, timeout, 
                                     src_spacetokendesc, dest_spacetokendesc);
            except Exception, e:
                status = 'UNKNOWN'
                summary = stMsg % ' NOT'
                self.printd('ERROR: %s\n%s' % (errmsg, str(e)))
            else:
                if rc != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                        summary = stMsg % (' NOT')+'[ErrDB:%s]' % str(er)
                    else:
                        status = 'CRITICAL'
                        summary = stMsg % ' NOT'
                    self.printd('ERROR: %s' % errmsg)
                else:
                    cmd = '`which diff` %s %s' % (self._fileTest, self._fileTestIn)
                    res = commands.getstatusoutput(cmd)
                    if res[0] == 0:
                        status = 'OK'
                        total_transfer = datetime.datetime.now()-start_transfer
                        self.printd('Transfer Duration: %s' % str(total_transfer))
                        summary = stMsg % ('')+' Diff successful.' + " Transfer time: "+str(total_transfer)
                    elif res[0] == 256: # files differ
                        status = 'CRITICAL'
                        summary = stMsg % ('')+' Files differ!'
                        self.printd('diff ERROR: %s' % res[1])
                    else:
                        status = 'UNKNOWN'
                        summary = stMsg % ''+' Unknown problem when comparing files!'
                        self.printd('diff ERROR: %s' % res[1])
            self.print_time()
            (self._voInfoDictionary[srmendpt])['getResult']=(status,summary)

        try:
            self.saveVoInfoDictionary(self._fileHistoryVoInfoDictionary)
        except IOError:
            self.printd('Error saving VO Info Dictionary to file %s' % self._fileHistoryVoInfoDictionary)

        #EXTRACT ARIBITRARY ITEM FROM THE DICTIONARY TO RETURN RESULTS
        #REPLACE WITH WEIGHTED CALCULATION BASED ON CRITICALITY OF PATHS/ENDPOINTS!!!

        weightedResult=self.weightEndpointCriticality('getResult')
        return weightedResult

#

    def metricDel(self):
        "Delete given file(s) from SRM."
        
        self.printd(self.lcg_util_gfal_ver)
        
        # TODO: - cleanup of the metric's working directory 
        #   (this may go to metricAll() in the superclass)
        
        # multiple Storage Paths are possible
        src_files = []
        try:
            for sfile in open(self._fileFilesOnSRM, 'r'):
                src_files.append(sfile.rstrip('\n'))
            if not src_files:
                return ('UNKNOWN', 'No files to depete from SRM found in %s' % 
                                    self._fileFilesOnSRM)
        except IOError, e:
            self.printd('ERROR: %s' % str(e))
            return ('UNKNOWN', 'Error opening local file.')

        # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
        # SRM types string to integer mapping
        # TYPE_NONE  -> 0
        # TYPE_SRM   -> 1
        # TYPE_SRMv2 -> 2
        # TYPE_SE    -> 3
        defaulttype = int(self.svcVer)
        setype      = defaulttype
        nobdii      = 1
        nolfc       = 1
        aflag       = 0
        se          = ''
        vo          = self.voName
        conf_file   = ''
        insecure    = 0
        verbose     = 0 # if self.verbosity > 0: verbose = 1 # when API is fixed
        timeout     = self._timeouts['srm_connect']
        
        self.printd('Using lcg_del4().')
        self.printd('''Parameters:
 defaulttype: %i
 setype: %i
 nobdii: %i
 nolfc: %i
 aflag: %i
 se: %s
 vo: %s
 conf_file: %s
 insecure: %i
 verbose: %i
 timeout: %i''' % (defaulttype, setype, nobdii, nolfc, aflag, 
                          se or '-', vo, conf_file or '-', insecure, 
                          verbose, timeout))
        
        stMsg = 'File was%s deleted from SRM.'
        for src_file in src_files:
            errmsg = ''
            self.print_time()
            self.printd('Deleting: %s' % src_file)
            try:
                rc, errmsg = \
                    lcg_util.lcg_del4(src_file, defaulttype, setype, nobdii, nolfc, aflag, 
                                      se, vo, conf_file, insecure, verbose, timeout);
            except Exception, e:
                status = 'UNKNOWN'
                summary = stMsg % ' NOT'
                self.printd('ERROR: %s\n%s' % (errmsg, str(e)))
            else:
                if rc != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                        summary = stMsg % (' NOT')+' [ErrDB:%s]' % str(er)
                    else:
                        status = 'CRITICAL'
                        summary  = stMsg % ' NOT'
                    self.printd('ERROR: %s' % errmsg)
                else:
                    status = 'OK'
                    summary = stMsg % ''
            self.print_time()

        return(status, summary)

    def metricVODel(self):
        "Delete given file(s) from SRM."
        
        self.printd(self.lcg_util_gfal_ver)
        
        # TODO: - cleanup of the metric's working directory 
        #   (this may go to metricAll() in the superclass)

        for srmendpt in self._voInfoDictionary.keys():
                        
            self.print_time()
            src_filename=(self._voInfoDictionary[srmendpt])['fn']
            src_file=srmendpt+'/'+src_filename

            self.printd('Source: %s' % src_file)

            # bug in lcg_util: https://gus.fzk.de/ws/ticket_info.php?ticket=39926
            # SRM types string to integer mapping
            # TYPE_NONE  -> 0
            # TYPE_SRM   -> 1
            # TYPE_SRMv2 -> 2
            # TYPE_SE    -> 3
            defaulttype = int(self.svcVer)
            setype      = defaulttype
            nobdii      = 1
            try:
                catalog=(self._voInfoDictionary[srmendpt])['fileCatalog']
                nolfc=0
            except KeyError:
                nolfc       = 1
            aflag       = 0
            se          = ''
            vo          = self.voName
            conf_file   = ''
            insecure    = 0
            verbose     = 0 # if self.verbosity > 0: verbose = 1 # when API is fixed
            timeout     = self._timeouts['srm_connect']
        
            self.printd('Using lcg_del4().')
            self.printd('''Parameters:
 defaulttype: %i
 setype: %i
 nobdii: %i
 nolfc: %i
 aflag: %i
 se: %s
 vo: %s
 conf_file: %s
 insecure: %i
 verbose: %i
 timeout: %i''' % (defaulttype, setype, nobdii, nolfc, aflag, 
                          se or '-', vo, conf_file or '-', insecure, 
                          verbose, timeout))
        
            stMsg = 'File was%s deleted from SRM.'
            errmsg = ''
            self.print_time()
            self.printd('Deleting: %s' % src_file)
            try:
                rc, errmsg = \
                    lcg_util.lcg_del4(src_file, defaulttype, setype, nobdii, nolfc, aflag, 
                                      se, vo, conf_file, insecure, verbose, timeout);
            except Exception, e:
                status = 'UNKNOWN'
                summary = stMsg % ' NOT'
                self.printd('ERROR: %s\n%s' % (errmsg, str(e)))
            else:
                if rc != 0:
                    em = probe.ErrorsMatching(self.errorDBFile, self.errorTopics)
                    er = em.match(errmsg)
                    if er:
                        status = er[0][2]
                        summary = stMsg % (' NOT')+' [ErrDB:%s]' % str(er)
                    else:
                        status = 'CRITICAL'
                        summary  = stMsg % ' NOT'
                    self.printd('ERROR: %s' % errmsg)
                else:
                    status = 'OK'
                    summary = stMsg % ''
            self.print_time()
            (self._voInfoDictionary[srmendpt])['delResult']=(status,summary)

        try:
            self.saveVoInfoDictionary(self._fileHistoryVoInfoDictionary)
        except IOError:
            self.printd('Error saving VO Info Dictionary to file %s' % self._fileHistoryVoInfoDictionary)

        #EXTRACT ARIBITRARY ITEM FROM THE DICTIONARY TO RETURN RESULTS
        #REPLACE WITH WEIGHTED CALCULATION BASED ON CRITICALITY OF PATHS/ENDPOINTS!!!

        try:
            return (self._voInfoDictionary.values()[0])['delResult']
        except IndexError:
            return ('UNKNOWN', 'No SRM endpoints found in internal dictionary')
        except KeyError:
            return ('UNKNOWN', 'No test results found in internal dictionary for SRM endpoint')
            
runner = probe.Runner(SRMVOMetrics, probe.ProbeFormatRenderer())
sys.exit(runner.run(sys.argv))
