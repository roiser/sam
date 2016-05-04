import logging
import urlparse

from ncgx.inventory import Checks, Hosts

HTTP_URIS = (
    'https://faketest',
)

WEBDAV_METRICS = (
    'webdav.HTTP-TLS_CIPHERS-/lhcb/Role=production',
    'webdav.HTTP-DIR_HEAD-/lhcb/Role=production',
    'webdav.HTTP-DIR_GET-/lhcb/Role=production',
    'webdav.HTTP-FILE_PUT-/lhcb/Role=production',
    'webdav.HTTP-FILE_GET-/lhcb/Role=production',
    'webdav.HTTP-FILE_OPTIONS-/lhcb/Role=production',
    'webdav.HTTP-FILE_MOVE-/lhcb/Role=production',
    'webdav.HTTP-FILE_HEAD-/lhcb/Role=production',
    'webdav.HTTP-FILE_HEAD_ON_NON_EXISTENT-/lhcb/Role=production',
    'webdav.HTTP-FILE_PROPFIND-/lhcb/Role=production',
    'webdav.HTTP-FILE_DELETE-/lhcb/Role=production',
    'webdav.HTTP-FILE_DELETE_ON_NON_EXISTENT-/lhcb/Role=production'
)

log = logging.getLogger('ncgx')


def run():
    log.info("Processing webdav LHCb feed: %s" % len(HTTP_URIS))
    h = Hosts()
    c = Checks()
    for uri in HTTP_URIS:
        puri = urlparse.urlparse(uri)
        if puri.hostname:
            c.add('webdav.HTTP-All-/lhcb/Role=production', hosts=(puri.hostname,),
                  params={'args': {'--uri': uri}, '_unique_tag': 'HTTPS'})
            h.add(puri.hostname, tags=('HTTPS',))
            for metric in WEBDAV_METRICS:
                c.add(metric, hosts=(puri.hostname,), params={'_unique_tag': 'HTTPS'})
    h.serialize(fname='/etc/ncgx/conf.d/generated_hosts_webdav.cfg')
    c.serialize(fname='/etc/ncgx/conf.d/generated_webdav.cfg')
