DEBUG = True
DEBUG_METHODS = False

MAP_JOBS_QUEUE = 'map_jobs'
REDUCE_JOBS_QUEUE = 'reduce_jobs'
MASTER_QUEUE = 'master_queue'

# maximum consummed messages by worker
MAX_MESSAGES = 1

MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017
# MongoDB db name
DB_NAME = 'mapreduce'

# RabbitMQ parameters
# used AMQP specification
# WARNING: 'delivery-mode' in 0.9.1 and 'delivery mode' in 0.8
SPEC_PATH = 'specs/standard/amqp0-9-1.stripped.xml'
RABBITMQ_HOST = '127.0.0.1'
RABBITMQ_VHOST = '/'
RABBITMQ_USERNAME = 'guest'
RABBITMQ_PASSWORD = 'guest'
RABBITMQ_PORT = 5672
# persistent delivery mode
PERSISTENT = 2
