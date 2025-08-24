# Codebase Overview

The Scrapy-Distributed project enables building distributed crawlers on top of Scrapy. It supplies scheduler, queue, middleware, pipelines, and duplicate filtering components that coordinate work across RabbitMQ, Kafka and RedisBloom.

## Repository Layout
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

## Key Components
- **DistributedScheduler** combines queue-based scheduling with a Redis Bloom duplicate filter.
- **RabbitQueue** and **KafkaQueue** serialize Scrapy requests to publish/consume through RabbitMQ or Kafka.
- **RedisBloomDupeFilter** tracks seen URLs using Redis Bloom filters.
- **RabbitMiddleware** and **RabbitPipeline** handle acknowledgement and item publishing.

## Example Usage
Example projects under `examples/` demonstrate how to configure the scheduler, queue, middleware and pipeline. Supporting services can be launched with the provided `docker-compose.dev.yaml`.

## Learning Path
1. Run the examples to see the distributed scheduler in action.
2. Review `schedulers` and `queues` modules to understand request flow.
3. Customize queue and Bloom filter settings via objects in `common` and `redis_utils`.
4. Extend middlewares or pipelines to integrate with additional services.
