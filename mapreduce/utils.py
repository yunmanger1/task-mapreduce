from twisted.internet.defer import inlineCallbacks, returnValue
# from twisted.internet.protocol import ClientCreator
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import Deferred
from twisted.python import log
# from twisted.internet.error import ConnectionDone

from txamqp.content import Content
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from settings import settings as ns
from Queue import Queue
from txrestapi.resource import APIResource
from twisted.python.failure import Failure
from twisted.internet.defer import FirstError

import txamqp.spec
from twisted.internet.defer import gatherResults
import time
import simplejson as json


def _tx(*a, **kw):
    return gatherResults(a, consumeErrors=kw.pop('consumeErrors', True))


class JsonAPIResource(APIResource):

    def _fail(self, request, data, message=None):
        message = message or 'INTERNAL_SERVER_ERROR'
        if isinstance(data, Failure):
            exceptionType = data.type.__name__
            exceptionExtra = data.getErrorMessage()
        else:
            exceptionType = data.__class__.__name__
            exceptionExtra = str(data)
        request.setHeader('Content-Type', 'application/json; charset=UTF-8')
        request.write(json.dumps({
            'status': 'error',
            'timestamp': time.time(),
            'data': {
                'exceptionType': exceptionType,
                'exceptionMessage': message,
                'exceptionExtra': exceptionExtra,
                }
            }))
        request.finish()

    def _done(self, data, request):
        self._ok(request, data)

    def _ok(self, request, data, *a):
        request.setHeader('Content-Type', 'application/json; charset=UTF-8')
        request.write(json.dumps({
            'status': 'ok',
            'timestamp': time.time(),
            'data': data}))
        request.finish()

    def handle_error(self, failure, request=None):
        while failure.type == FirstError:
            failure = failure.value.subFailure
        log.err(failure)
        if request and not request.finished:
            self._fail(request, failure)


class AMQPProtocol(AMQClient):

    def connectionMade(self):
        AMQClient.connectionMade(self)
        self.factory.gotConnection(self)

    def connectionLost(self, failure):
        AMQClient.connectionLost(self, failure)
        self.factory.lostConnection(failure)


class AMQPReconnectingFactory(ReconnectingClientFactory):

    protocol = AMQPProtocol

    def __init__(self, spec_path=None, vhost='/', username='guest', \
        password='guest', host='localhost', port=5672, delegate=None):
        self.username = username
        self.password = password
        self.delegate = delegate or TwistedDelegate()
        self.vhost = vhost
        self.spec = txamqp.spec.load(spec_path)
        self.is_connected = False
        self.host = host
        self.port = port

    def buildProtocol(self, addr):
        p = self.protocol(self.delegate, self.vhost, self.spec)
        p.factory = self
        self.deferred = Deferred()
        self.addCallbacksOnProtocol(self.deferred)
        self.deferred.addErrback(log.err)
        return p

    @inlineCallbacks
    def gotConnection(self, connection):
        log.msg(' Got connection, authenticating.')
        self.resetDelay()
        yield connection.start({'LOGIN': self.username,
                          'PASSWORD': self.password})
        channel = yield connection.channel(1)
        yield channel.channel_open()
        self.is_connected = True
        self.deferred.callback((connection, channel))

    def lostConnection(self, failure):
        self.is_connected = False

    def addCallbacksOnProtocol(self, deferred):
        """
        This deferrred will fire on new connection, channel.
        """
        self.deferred.addCallback(lambda *a, **kw: log.msg('Got new channel'))

    def startAMQPConnection(self):
        from twisted.internet import reactor
        reactor.connectTCP(self.host, self.port, self)


class QueueConsumer(AMQPReconnectingFactory):
    """
    Class implmenting functionality of pool based queue message consuming.
    """

    pool = None
    pool_size = 0
    message_handler = None

    def __init__(self, consumer_queue, pool_size=5, **kw):
        AMQPReconnectingFactory.__init__(self, **kw)
        self.pool_size = pool_size
        self.pool = [None] * pool_size
        self.consumer_queue = consumer_queue

    def _ack(self, msg):
        """
        acknowledging that message is received and successfully processed.
        """
        if msg is not None:
            self.consumer_channel.basic_ack(delivery_tag=msg.delivery_tag)
            return msg
        else:
            log.err('Msg processing is not successfull: no ack')

    @inlineCallbacks
    def message_flow(self, msg):
        """
        This method defines message flow. If you need to change the flow,
        feel free to override this method.
        """
        if self.message_handler:
            msg = yield self.message_handler.process_message(msg)
            msg = yield self._ack(msg)
        else:
            yield 1
        returnValue(msg)

    @inlineCallbacks
    def _run_msg_flow(self, index, d):
        """
        inserts deferred `d` into pool at position `index`.
        Adding callbacks flow.

        When message received it's processed by `process_message` method.
        On processing success acknowledgement is sent.

        After getting another deferred from queue, to repeat the flow.
        """
        self.pool[index] = d
        try:
            msg = yield d
        except txamqp.queue.Closed:
            log.msg('Connection closed, exiting loop')
            returnValue(None)

        try:
            yield self.message_flow(msg)
        except txamqp.queue.Closed:
            log.msg('Connection closed, exiting loop')
            returnValue(None)
        except:
            log.err()
        yield self._renew_deferred(index, self.queue)

    def _renew_deferred(self, index, queue):
        """
        gets another queue.get deferred and inserts it into pool
        """
        d = queue.get()
        self._run_msg_flow(index, d)

    def addCallbacksOnProtocol(self, deferred):
        deferred.addCallback(self.consume)

    @inlineCallbacks
    def consume(self, result):
        """
        Initializes consumers pool.
        """
        self.consumer_connection, self.consumer_channel = \
            connection, channel = result
        queue_name = self.consumer_queue
        # declare queue if not created.
        yield channel.queue_declare(queue=queue_name, durable=True)
        if ns.DEBUG:
            log.msg(' [*] Waiting for messages. To exit press CTRL+C')
        # allows to fetch pool_size messages at once
        yield channel.basic_qos(prefetch_count=self.pool_size)
        subscription = yield channel.basic_consume(queue=queue_name)
        self.queue = queue = yield connection.queue(subscription.consumer_tag)
        for i in xrange(self.pool_size):
            # getting message from the queue deferred
            d = queue.get()
            # adding deferred to pool
            self._run_msg_flow(i, d)
        returnValue(result)

    def process_message(self, msg, index):
        if self.message_handler:
            return self.message_handler.process_message(msg)
        return None


class QueuePublisher(AMQPReconnectingFactory):
    """
    Class implmenting functionality of sending message to queue.
    """
    publisher_conn, publisher_channel = None, None

    def __init__(self, declare_strategy=1, **kw):
        """
        declare_strategy:
            0 never declare
            1 declare once
            2 declare all the time
        """
        AMQPReconnectingFactory.__init__(self, **kw)
        self.messages = Queue()
        self.declare_strategy = declare_strategy
        self.declared = False

    @inlineCallbacks
    def close_channel(self, result):
        """
        close channel and connection
        """
        connection, channel = result
        yield channel.channel_close()
        chan0 = yield connection.channel(0)
        yield chan0.connection_close()

    def addCallbacksOnProtocol(self, deferred):
        deferred.addCallback(self.refresh_and_publish)

    def refresh_and_publish(self, result):
        self.publisher_connection, self.publisher_channel = result
        self.declared = False
        self.try_to_sendout()

    @inlineCallbacks
    def publish(self, message, routing_key=None, headers={}, \
        persistent=True, exchange_name=None, exchange_type=None):
        """
        publish `message` into `routing_key` queue.
        if not set uses `self.publisher_queue`
        """
        routing_key = routing_key or self.publisher_queue
        channel = self.publisher_channel
        exchange_name = exchange_name or ''
        if self.declare_strategy == 2 or \
            (self.declare_strategy == 1 and not self.declared):
            if exchange_name:
                yield channel.exchange_declare(exchange=exchange_name, \
                    type=exchange_type or 'topic')
            else:
                # saying that queue is durable and will survive reload
                yield channel.queue_declare(queue=routing_key,\
                    durable=persistent)
            self.declared = True
        msg = Content(message)
        # saying that message is persistent and will survive reload
        msg["delivery-mode"] = persistent and 2 or 1
        if headers:
            msg["headers"] = headers
        yield channel.basic_publish(
            exchange=exchange_name, routing_key=routing_key, content=msg)
        if ns.DEBUG:
            log.msg(' [x] Sent "%s" with routing_key [%s]' % \
                (message, routing_key))

    @inlineCallbacks
    def try_to_sendout(self):
        while self.is_connected and not self.messages.empty():
            a, kw = self.messages.get()
            yield self.publish(*a, **kw)

    def sendMessage(self, *a, **kw):
        """
        opens connection and sends message.
        """
        self.messages.put((a, kw))
        return self.try_to_sendout()
