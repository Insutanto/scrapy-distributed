# **Scrapy-Distributed**

`Scrapy-Distributed` is a series of components for you to develop a distributed crawler base on `Scrapy` in an easy way.

Now! `Scrapy-Distributed` has supported `RabbitMQ Scheduler`, `Kafka Scheduler` and `RedisBloom DupeFilter`. You can use either of those in your Scrapy's project very easily.

## **Features**

- RabbitMQ Scheduler
    - Support custom declare a RabbitMQ's Queue. Such as `passive`, `durable`, `exclusive`, `auto_delete`, and all other options.
- RabbitMQ Pipeline
    - Support custom declare a RabbitMQ's Queue for the items of spider. Such as `passive`, `durable`, `exclusive`, `auto_delete`, and all other options.
- Kafaka Scheduler
    - Support custom declare a Kafka's Topic. Such as `num_partitions`, `replication_factor` and will support other options.
- RedisBloom DupeFilter
    - Support custom the `key`, `errorRate`, `capacity`, `expansion` and auto-scaling(`noScale`) of a bloom filter.

## **Requirements**

- Python >= 3.6
- Scrapy >= 1.8.0
- Pika >= 1.0.0
- RedisBloom >= 0.2.0
- Redis >= 3.0.1
- kafka-python >= 1.4.7

## **TODO**

- ~~RabbitMQ Item Pipeline~~
- Support Delayed Message in RabbitMQ Scheduler
- Support Scheduler Serializer
- Custom Interface for DupeFilter
- RocketMQ Scheduler
- RocketMQ Item Pipeline
- SQLAlchemy Item Pipeline
- Mongodb Item Pipeline
- ~~Kafka Scheduler~~
- ~~Kafka Item Pipeline~~

## **Usage**

### **Step 0:**

```
pip install scrapy-distributed
```

OR

```
git clone https://github.com/Insutanto/scrapy-distributed.git && cd scrapy-distributed
&& python setup.py install
```

There is a simple demo in [`examples/simple_example`]((examples/)). Here is the fast way to use `Scrapy-Distributed`.

### [Examples of RabbitMQ](examples/rabbitmq_example)

If you don't have the required environment for tests:

```bash
# pull and run a RabbitMQ container.
docker run -d --name rabbitmq -p 0.0.0.0:15672:15672 -p 0.0.0.0:5672:5672 rabbitmq:3-management

# pull and run a RedisBloom container.
docker run -d --name redisbloom -p 6379:6379 redis/redis-stack

cd examples/rabbitmq_example
python run_simple_example.py
```

Or you can use docker compose:

```bash
docker compose -f ./docker-compose.dev.yaml up -d
cd examples/rabbitmq_example
python run_simple_example.py
```

### [Examples of Kafka](examples/kafka_example)

If you don't have the required environment for tests:

```bash
# make sure you have a Kafka running on localhost:9092
# pull and run a RedisBloom container.
docker run -d --name redisbloom -p 6379:6379 redis/redis-stack

cd examples/kafka_example
python run_simple_example.py
```

Or you can use docker compose:

```bash
docker compose -f ./docker-compose.dev.yaml up -d
cd examples/kafka_example
python run_simple_example.py
```

## RabbitMQ Support

If you don't have the required environment for tests:

```bash
# pull and run a RabbitMQ container.
docker run -d --name rabbitmq -p 0.0.0.0:15672:15672 -p 0.0.0.0:5672:5672 rabbitmq:3-management

# pull and run a RedisBloom container.
docker run -d --name redisbloom -p 6379:6379 redis/redis-stack
```

Or you can use docker compose:

```bash
docker compose -f ./docker-compose.dev.yaml up -d
```

### **Step 1:**

Only by change `SCHEDULER`, `DUPEFILTER_CLASS` and add some configs, you can get a distributed crawler in a moment.

```
SCHEDULER = "scrapy_distributed.schedulers.DistributedScheduler"
SCHEDULER_QUEUE_CLASS = "scrapy_distributed.queues.amqp.RabbitQueue"
RABBITMQ_CONNECTION_PARAMETERS = "amqp://guest:guest@localhost:5672/example/?heartbeat=0"
DUPEFILTER_CLASS = "scrapy_distributed.dupefilters.redis_bloom.RedisBloomDupeFilter"
BLOOM_DUPEFILTER_REDIS_URL = "redis://:@localhost:6379/0"
BLOOM_DUPEFILTER_REDIS_HOST = "localhost"
BLOOM_DUPEFILTER_REDIS_PORT = 6379
REDIS_BLOOM_PARAMS = {
    "redis_cls": "redisbloom.client.Client"
}
BLOOM_DUPEFILTER_ERROR_RATE = 0.001
BLOOM_DUPEFILTER_CAPACITY = 100_0000

# disable the RedirectMiddleware, because the RabbitMiddleware can handle those redirect request.
DOWNLOADER_MIDDLEWARES = {
    ...
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": None,
    "scrapy_distributed.middlewares.amqp.RabbitMiddleware": 542
}

# add RabbitPipeline, it will push your items to rabbitmq's queue. 
ITEM_PIPELINES = {
    ...
   'scrapy_distributed.pipelines.amqp.RabbitPipeline': 301,
}


```

### **Step 2:**

```
scrapy crawl <your_spider>
```

## Kafka Support

### **Step 1:**
```
SCHEDULER = "scrapy_distributed.schedulers.DistributedScheduler"
SCHEDULER_QUEUE_CLASS = "scrapy_distributed.queues.kafka.KafkaQueue"
KAFKA_CONNECTION_PARAMETERS = "localhost:9092"
DUPEFILTER_CLASS = "scrapy_distributed.dupefilters.redis_bloom.RedisBloomDupeFilter"
BLOOM_DUPEFILTER_REDIS_URL = "redis://:@localhost:6379/0"
BLOOM_DUPEFILTER_REDIS_HOST = "localhost"
BLOOM_DUPEFILTER_REDIS_PORT = 6379
REDIS_BLOOM_PARAMS = {
    "redis_cls": "redisbloom.client.Client"
}
BLOOM_DUPEFILTER_ERROR_RATE = 0.001
BLOOM_DUPEFILTER_CAPACITY = 100_0000

DOWNLOADER_MIDDLEWARES = {
    ...
   "scrapy_distributed.middlewares.kafka.KafkaMiddleware": 542
}

```

### **Step 2:**

```
scrapy crawl <your_spider>
```

## **Reference Project**

`scrapy-rabbitmq-link`([scrapy-rabbitmq-link](https://github.com/mbriliauskas/scrapy-rabbitmq-link))

`scrapy-redis`([scrapy-redis](https://github.com/rmax/scrapy-redis))