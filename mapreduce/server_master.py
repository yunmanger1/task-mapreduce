# coding: utf-8
from txrestapi.methods import POST, GET
from twisted.web.server import NOT_DONE_YET
from twisted.internet.defer import Deferred
from twisted.python import log
from utils import QueueConsumer, QueuePublisher
# from twisted.internet.defer import inlineCallbacks
from swtools.twistd.resource import JsonAPIResource
from twisted.web.server import Site
from twisted.application import internet
from settings import settings as ns
import simplejson as json


class MasterResource(JsonAPIResource):
    tasks = {}

    def init(self, consumer, publisher):
        consumer.message_handler = self
        consumer.startAMQPConnection()
        publisher.startAMQPConnection()
        self.publisher = publisher
        self.consumer = consumer

    def run_task(self, task, data):
        headers = {'task': task}
        count = 0
        for chunk in self.split(data):
            self.publisher.sendMessage(chunk,
                routing_key=ns.MAP_JOBS_QUEUE,
                headers=headers)
            count += 1
        d = Deferred()
        self.tasks[task] = {
            'sub': count,
            'map_complete': 0,
            'status': 'ON_MAP',
            'd': d
        }
        return d

    def process_message(self, msg):
        headers = 'headers' in msg.content.properties and \
            msg.content.properties['headers'] or None

        data = msg.content.body

        log.msg('RECEIVED MSG: {}'.format(str(headers)))
        log.msg(data)

        task = headers.get('task')
        worker = headers.get('worker')
        if worker == 'map':
            obj = self.tasks.get(task)
            if obj['status'] == 'ON_MAP':
                obj['map_complete'] += 1
                if obj['map_complete'] >= obj['sub']:
                    obj['status'] == 'ON_REDUCE'
                    self.publisher.sendMessage(task,
                        routing_key=ns.REDUCE_JOBS_QUEUE,
                        headers={'task': task})
        elif worker == 'reduce':
            obj = json.loads(data)
            d = task['d']
            d.callback(obj)

    @POST('^/task/')
    def task_view(self, request):
        task = request.args.get('task', ['default'])[0]
        data = request.content.getvalue()
        d = self.run_task(task, data)
        d.addCallback(self._done, request)
        return NOT_DONE_YET

    @GET('^/status/')
    def status_view(self, request):
        return "NOT IMPLEMENTED YET"


def Service(options):
    consumer = QueueConsumer(
        consumer_queue=ns.MASTER_QUEUE,
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
    resource = MasterResource()
    resource.init(consumer, publisher)
    site = Site(resource)
    site.displayTracebacks = ns.DEBUG
    return internet.TCPServer(int(options['port']), site, \
        interface=options['iface'])
