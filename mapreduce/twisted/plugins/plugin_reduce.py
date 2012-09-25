from twisted.plugin import IPlugin
from zope.interface import implements
from twisted.application import service
from twisted.python import usage

from worker_reduce import Service


class Options(usage.Options):
    optParameters = [
        ['hosts', 'h', '127.0.0.1', 'Database hosts'],
        # ['iface', 'i', '0.0.0.0', 'The interface to listen on.'],
    ]


class ServiceMaker(object):

    implements(service.IServiceMaker, IPlugin)

    tapname = "mr.reduce"
    description = "MapReduce reduce worker plugin."
    options = Options

    def makeService(self, options):
        options['hosts'] = options.get('hosts').split(',')
        print options
        return Service(options)

service_maker = ServiceMaker()
