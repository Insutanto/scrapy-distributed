# **Scrapy-Distributed**

`Scrapy-Distributed` is a series of components for you to develop a distributed crawler base on `Scrapy` in an easy way.

Now! `Scrapy-Distributed` has supported `RabbitMQ Scheduler`, `Kafka Scheduler` and `RedisBloom DupeFilter`. You can use either of those in your Scrapy's project very easily.

## **Features**

- RabbitMQ Scheduler
    - Support custom declare a RabbitMQ's Queue. Such as `passive`, `durable`, `exclusive`, `auto_delete`, and all other options.
- RabbitMQ Pipeline
    - Support custom declare a RabbitMQ's Queue for the items of spider. Such as `passive`, `durable`, `exclusive`, `auto_delete`, and all other options.
- Kafka Scheduler
    - Support custom declare a Kafka's Topic. Such as `num_partitions`, `replication_factor` and will support other options.
- RedisBloom DupeFilter
    - Support custom the `key`, `errorRate`, `capacity`, `expansion` and auto-scaling(`noScale`) of a bloom filter.
- Custom DupeFilter Interface
    - Implement your own deduplication logic by extending `BaseDupeFilter`.
- SQLAlchemy Pipeline
    - Persist scraped items to any relational database supported by SQLAlchemy (SQLite, PostgreSQL, MySQL, etc.).  The table schema is inferred automatically from item fields.

## **Requirements**

- Python >= 3.6
- Scrapy >= 1.8.0
- Pika >= 1.0.0
- RedisBloom >= 0.2.0
- Redis >= 3.0.1
- kafka-python >= 1.4.7
- SQLAlchemy >= 1.4.0

## **TODO**

- ~~RabbitMQ Item Pipeline~~
- Support Delayed Message in RabbitMQ Scheduler
- Support Scheduler Serializer
- ~~Custom Interface for DupeFilter~~
- RocketMQ Scheduler
- ~~RocketMQ Item Pipeline~~
- ~~SQLAlchemy Item Pipeline~~
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

There are example projects in `examples/rabbitmq_example`, `examples/kafka_example` and `examples/rocketmq_example`. Here is the fast way to use `Scrapy-Distributed`.

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

### [Examples of RocketMQ](examples/rocketmq_example)

If you don't have the required environment for tests:

```bash
# make sure you have a RocketMQ name server running on localhost:9876
# pull and run a RedisBloom container.
docker run -d --name redisbloom -p 6379:6379 redis/redis-stack

cd examples/rocketmq_example
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


## RocketMQ Pipeline

The `RocketMQPipeline` publishes scraped items to a RocketMQ topic.

### **Step 1:**

```
SCHEDULER = "scrapy_distributed.schedulers.DistributedScheduler"
SCHEDULER_QUEUE_CLASS = "scrapy_distributed.queues.rocketmq.RocketMQQueue"
ROCKETMQ_NAME_SERVER = "localhost:9876"
DUPEFILTER_CLASS = "scrapy_distributed.dupefilters.redis_bloom.RedisBloomDupeFilter"
BLOOM_DUPEFILTER_REDIS_URL = "redis://:@localhost:6379/0"
BLOOM_DUPEFILTER_REDIS_HOST = "localhost"
BLOOM_DUPEFILTER_REDIS_PORT = 6379
REDIS_BLOOM_PARAMS = {
    "redis_cls": "redisbloom.client.Client"
}
BLOOM_DUPEFILTER_ERROR_RATE = 0.001
BLOOM_DUPEFILTER_CAPACITY = 100_0000

# add RocketMQPipeline, it will push your items to a RocketMQ topic.
ITEM_PIPELINES = {
    ...
   'scrapy_distributed.pipelines.rocketmq.RocketMQPipeline': 301,
}
```

You can customise the topic, consumer group, tags, and keys used for items by
setting an `item_conf` attribute on your spider:

```python
from scrapy_distributed.common.queue_config import RocketMQQueueConfig

class MySpider(SitemapSpider):
    name = "myspider"
    item_conf = RocketMQQueueConfig(
        topic="myspider.items",
        group="myspider.items",
        tags="scraped",
    )
```

If no `item_conf` is provided the pipeline defaults to a topic named
`<spider_name>.items` with the group `default`.

### **Step 2:**

```
scrapy crawl <your_spider>
```


## SQLAlchemy Pipeline

The `SqlAlchemyPipeline` stores scraped items into any relational database
supported by SQLAlchemy (SQLite, PostgreSQL, MySQL, …).  The target table is
created automatically the first time the spider runs.  All item fields are
stored as `Text` columns.  New fields encountered in later items are added to
the table automatically via `ALTER TABLE`.

### **Step 1:**

```python
# settings.py

# Any SQLAlchemy-compatible connection URL.
SQLALCHEMY_CONNECTION_STRING = "sqlite:///items.db"

# Optional: override the default table name ("<spider_name>_items").
# SQLALCHEMY_TABLE_NAME = "my_table"

ITEM_PIPELINES = {
    "scrapy_distributed.pipelines.sqlalchemy.SqlAlchemyPipeline": 300,
}
```

### **Step 2:**

```
scrapy crawl <your_spider>
```

Items are written to the configured database table as the spider runs.

---

## Custom DupeFilter

You can implement your own deduplication logic by extending
`scrapy_distributed.dupefilters.BaseDupeFilter`.

### **Step 1: Implement `BaseDupeFilter`**

```python
from scrapy_distributed.dupefilters import BaseDupeFilter


class MyDupeFilter(BaseDupeFilter):

    @classmethod
    def from_settings(cls, settings):
        return cls()

    @classmethod
    def from_spider(cls, spider):
        instance = cls.from_settings(spider.settings)
        # read spider-specific config, e.g. spider.dupefilter_conf
        return instance

    def request_seen(self, request):
        # return True if the request was already seen
        ...

    def open(self, spider=None):
        pass

    def close(self, reason=""):
        pass
```

### **Step 2: Register the filter in settings**

```python
SCHEDULER = "scrapy_distributed.schedulers.DistributedScheduler"
DUPEFILTER_CLASS = "myproject.dupefilters.MyDupeFilter"
```

The `DistributedScheduler` will call `MyDupeFilter.from_spider(spider)` during
startup so that the filter can read any spider-level configuration it needs.

## **Reference Project**

`scrapy-rabbitmq-link`([scrapy-rabbitmq-link](https://github.com/mbriliauskas/scrapy-rabbitmq-link))

`scrapy-redis`([scrapy-redis](https://github.com/rmax/scrapy-redis))

## Codebase Overview

The Scrapy-Distributed project enables building distributed crawlers on top of Scrapy. It supplies scheduler, queue, middleware, pipelines, and duplicate filtering components that coordinate work across RabbitMQ, Kafka and RedisBloom.

### Repository Layout
- `scrapy_distributed/` – library modules  
    - `amqp_utils/` – RabbitMQ helpers  
    - `common/` – queue configuration objects  
    - `dupefilters/` – Redis Bloom-based duplicate filter  
    - `middlewares/` – downloader middlewares for ACK/requeue  
    - `pipelines/` – item pipelines to publish items to queues  
    - `queues/` – RabbitMQ and Kafka queue implementations  
    - `redis_utils/` – Redis connection helpers  
    - `schedulers/` – distributed scheduler combining queue and dupe filter  
    - `spiders/` – mixins and example spiders
- `examples/` – small Scrapy projects showing how to use RabbitMQ and Kafka
- `tests/` – unit tests for key components

### Key Components
- **DistributedScheduler** combines queue-based scheduling with a Redis Bloom duplicate filter.
- **RabbitQueue** and **KafkaQueue** serialize Scrapy requests to publish/consume through RabbitMQ or Kafka.
- **RedisBloomDupeFilter** tracks seen URLs using Redis Bloom filters.
- **RabbitMiddleware** and **RabbitPipeline** handle acknowledgement and item publishing for RabbitMQ.
- **KafkaPipeline** publishes scraped items to a Kafka topic.
- **RocketMQPipeline** publishes scraped items to a RocketMQ topic.
- **SqlAlchemyPipeline** persists scraped items to any SQLAlchemy-supported relational database.

### Example Usage
Example projects under `examples/` demonstrate how to configure the scheduler, queue, middleware and pipeline. Supporting services can be launched with the provided `docker-compose.dev.yaml`.

### Learning Path
1. Run the examples to see the distributed scheduler in action.
2. Review `schedulers` and `queues` modules to understand request flow.
3. Customize queue and Bloom filter settings via objects in `common` and `redis_utils`.
4. Extend middlewares or pipelines to integrate with additional services.
