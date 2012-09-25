from twisted.plugin import IPlugin
from zope.interface import implements
from twisted.application import service
from twisted.python import usage

from server_master import Service


class Options(usage.Options):
    optParameters = [
        ['port', 'p', 8080, 'The port number to listen on.'],
        ['iface', 'i', '0.0.0.0', 'The interface to listen on.'],
    ]


class ServiceMaker(object):

    implements(service.IServiceMaker, IPlugin)

    tapname = "mr.master"
    description = "MapReduce Master service plugin."
    options = Options

    def makeService(self, options):
        return Service(options)

service_maker = ServiceMaker()
