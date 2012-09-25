# coding: utf-8
from twisted.internet.defer import inlineCallbacks, returnValue
# from twisted.internet import reactor
# from zope.interface import implements
from twisted.application import service
from settings import settings as ns
import simplejson as json
from swtools.queue import QueueConsumer, QueuePublisher
from twisted.python import log
# from remotes import remotes
# import sys
from txmongo.collection import SON
# from swtools.twistd import _tx
# from swtools.json import o2js
# from txyam.client import NoServerError
import txmongo
from txmongo import filter


class MapWorker(object):
    """
    Responsible for fetching messages from feeds queue and performing:
    create
    update
    delete operations
    """

    def __init__(self, consumer, publisher, db):
        consumer.message_handler = self
        consumer.startAMQPConnection()
        publisher.startAMQPConnection()
        self.publisher = publisher
        self.consumer = consumer
        self.db = db

    @property
    def results(self):
        return self.db['map_results']

    @inlineCallbacks
    def process_message(self, msg):
        headers = 'headers' in msg.content.properties and \
            msg.content.properties['headers'] or None

        data = msg.content.body

        log.msg('RECEIVED MSG: {}'.format(str(headers)))
        log.msg(data)

        part = {}
        for key, value in self.run(data, headers):
            ap = part.get(key, None)
            if ap is None:
                ap = []
                part[key] = ap
            ap.append((key, value))

        data = dict(**headers)
        data['result'] = json.dumps(part)
        yield self.results.insert(SON(data))

        # TODO: I think this is not right place for this code. @german
        idx = filter.sort(filter.ASCENDING("task"))
        self.results.ensure_index(idx)
        headers['worker'] = 'map'
        self.publisher.sendMessage('OK',
            routing_key=ns.MASTER_QUEUE,
            headers=headers)
        returnValue(msg)


class Service(service.Service):

    def __init__(self, options):
        self.options = options

    def startService(self):
        consumer = QueueConsumer(
            consumer_queue=ns.MAP_JOBS_QUEUE,
            pool_size=ns.MAX_MESSAGES,
            spec_path=ns.SPEC_PATH,
            vhost=ns.RABBITMQ_VHOST,
            username=ns.RABBITMQ_USERNAME,
            password=ns.RABBITMQ_PASSWORD,
            host=ns.RABBITMQ_HOST,
            port=ns.RABBITMQ_PORT
            )

        publisher = QueuePublisher(
            declare_strategy=1,
            spec_path=ns.SPEC_PATH,
            vhost=ns.RABBITMQ_VHOST,
            username=ns.RABBITMQ_USERNAME,
            password=ns.RABBITMQ_PASSWORD,
            host=ns.RABBITMQ_HOST,
            port=ns.RABBITMQ_PORT
            )

        # gettings mongo db collection pool
        _db = txmongo.lazyMongoConnectionPool(\
            host=ns.MONGODB_HOST, port=ns.MONGODB_PORT)

        MapWorker(consumer, publisher, _db[ns.DB_NAME])

    def stopService(self):
        pass
