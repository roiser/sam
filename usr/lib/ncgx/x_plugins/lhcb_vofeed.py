import logging

from ncgx.inventory import Hosts, Checks
from vofeed.api import VOFeed

log = logging.getLogger('ncgx')

WN_METRICS = (
'emi.cream.glexec.WN-gLExec-/lhcb/Role=pilot',
'org.lhcb.WN-cvmfs-/lhcb/Role=production',
'org.lhcb.WN-mjf-/lhcb/Role=production',
'org.lhcb.WN-sft-csh-/lhcb/Role=production',
'org.lhcb.WN-sft-lcg-rm-gfal-/lhcb/Role=production',
'org.lhcb.WN-sft-vo-swdir-/lhcb/Role=production',
'org.lhcb.WN-sft-voms-/lhcb/Role=production'
)

ARC_METRICS = (
'org.sam.ARC-JobSubmit-/lhcb/Role=pilot',
'org.sam.ARC-JobSubmit-/lhcb/Role=production'
)

CREAM_METRICS = (
'emi.cream.glexec.CREAMCE-DirectJobSubmit-/lhcb/Role=pilot',
'emi.cream.CREAMCE-DirectJobSubmit-/lhcb/Role=production'
)

SRM_METRICS = (
'org.lhcb.SRM-AllLHCb-/lhcb/Role=production',
'org.lhcb.SRM-GetLHCbInfo-/lhcb/Role=production',
'org.lhcb.SRM-VODel-/lhcb/Role=production',
'org.lhcb.SRM-VOGet-/lhcb/Role=production',
'org.lhcb.SRM-VOLs-/lhcb/Role=production',
'org.lhcb.SRM-VOLsDir-/lhcb/Role=production',
'org.lhcb.SRM-VOPut-/lhcb/Role=production')


def run(url):
    log.info("Processing vo feed: %s" % url)

    # Get services from the VO feed, i.e 
    # list of tuples (hostname, flavor, endpoint)
    services = VOFeed(url).get_services()
    
    # Add hosts, each tagged with corresponding flavors
    # creates /etc/ncgx/conf.d/generated_hosts.cfg
    h = Hosts()
    for service in services:
        h.add(service[0], tags=[service[1]])
    h.serialize()

    # Add corresponding metrics to tags
    # creates /etc/ncgx/conf.d/generated_checks.cfg
    c = Checks()
    c.add_all(CREAM_METRICS, tags=["CREAM-CE",])
    c.add_all(ARC_METRICS, tags=["ARC-CE",])
    c.add_all(WN_METRICS, tags=["ARC-CE", "CREAM-CE"])
    c.add_all(SRM_METRICS, tags=["SRMv2",])
    c.serialize()

