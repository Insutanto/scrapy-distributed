# **Scrapy-Distributed**

`Scrapy-Distributed` is a series of components for you to develop a distributed crawler base on `Scrapy` in an easy way.

Now! `Scrapy-Distributed` has supported `RabbitMQ Scheduler` and `RedisBloom DupeFilter`. You can use either of those in your Scrapy's project very easily.

## **Features**

- RabbitMQ Scheduler
    - Support custom declare a RabbitMQ's Queue. Such as `passive`, `durable`, `exclusive`, `auto_delete`, and all other options.
- RedisBloom DupeFilter
    - Support custom the `key`, `errorRate`, `capacity`, `expansion` and auto-scaling(`noScale`) of a bloom filter.

## **Requirements**

- Python >= 3.6
- Scrapy >= 1.8.0
- Pika >= 1.0.0
- RedisBloom >= 0.2.0
- Redis >= 3.0.1

## **Usage**

There is a simple demo in `examples/simple_example`. Here is the fast way to use `Scrapy-Distributed`.

### **Step 0:**

```
pip install scrapy-distributed
```

OR

```
git clone https://github.com/Insutanto/scrapy-distributed.git && cd scrapy-distributed
&& python setup.py install
```

If you don't have the required environment for tests:

```bash
# pull and run a RabbitMQ container.
docker run -d --name rabbitmq -p 0.0.0.0:15672:15672 -p 0.0.0.0:5672:5672 rabbitmq:3
# pull and run a RedisBloom container.
docker run -d --name redis-redisbloom -p 6379:6379 redislabs/rebloom:latest
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

```

### **Step 2:**

```
scrapy crawl <your_spider>
```

## **TODO**

- RabbitMQ Item Pipeline
- Support Delayed Message in RabbitMQ Scheduler
- Support Scheduler Serializer
- Custom Interface for DupeFilter
- RocketMQ Scheduler
- RocketMQ Item Pipeline
- Kafka Scheduler
- Kafka Item Pipeline

## **Reference Project**

`scrapy-rabbitmq-link`([scrapy-rabbitmq-link](https://github.com/mbriliauskas/scrapy-rabbitmq-link))

`scrapy-redis`([scrapy-redis](https://github.com/rmax/scrapy-redis))