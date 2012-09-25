# coding: utf-8
from twisted.internet.defer import inlineCallbacks, returnValue
# from twisted.internet import reactor
# from zope.interface import implements
from twisted.application import service
from settings import settings as ns
import simplejson as json
from utils import QueueConsumer, QueuePublisher
from twisted.python import log
# import sys
from txmongo.collection import SON
from utils import _tx
import txmongo
# from txmongo import filter


class ReduceWorker(object):
    """
    Responsible for fetching messages from feeds queue and performing:
    create
    update
    delete operations
    """

    def __init__(self, consumer, publisher, mongo_pool):
        consumer.message_handler = self
        consumer.startAMQPConnection()
        publisher.startAMQPConnection()
        self.publisher = publisher
        self.pool = mongo_pool

    @inlineCallbacks
    def process_message(self, msg):
        """
        spray message to all friends of author
        """
        headers = 'headers' in msg.content.properties and \
            msg.content.properties['headers'] or None

        log.msg('RECEIVED MSG: {}'.format(str(headers)))
        # log.msg(data)

        task = headers.get('task')
        defereds = []
        for db in self.pool.values():
            d = db[ns.DB_NAME]['map_results'].find(SON({"task": task}))
            defereds.append(d)

        results = yield _tx(*defereds)

        # join results
        data = {}
        for result in results:
            dtx = json.loads(result.get('result'))
            for key, value in dtx.iteritems():
                lst = data.get(key, [])
                lst.extend(value)
                data[key] = lst

        data2 = {}
        for key, value in data.iteritems():
            data2[key] = self.reduce(key, value)

        headers['worker'] = 'reduce'
        self.publisher.sendMessage(json.dumps(data2),
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

        hosts = self.options.get('hosts')
        mongo_pool = {}
        for host in hosts:
            mongo_pool[host] = txmongo.lazyMongoConnectionPool(\
                host=host, port=ns.MONGODB_PORT)

        ReduceWorker(consumer, publisher, mongo_pool)

    def stopService(self):
        pass
