from twisted.plugin import IPlugin
from zope.interface import implements
from twisted.application import service
from twisted.python import usage

from worker_map import Service


class Options(usage.Options):
    optParameters = [
        # ['collection', 'c', None, 'The database collection name.'],
        # ['iface', 'i', '0.0.0.0', 'The interface to listen on.'],
    ]


class ServiceMaker(object):

    implements(service.IServiceMaker, IPlugin)

    tapname = "mr.map"
    description = "Map Reduce map worker plugin."
    options = Options

    def makeService(self, options):
        return Service(options)

service_maker = ServiceMaker()
