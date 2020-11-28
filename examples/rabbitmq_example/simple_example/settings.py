# Scrapy settings for simple_example project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "simple_example"

SPIDER_MODULES = ["simple_example.spiders"]
NEWSPIDER_MODULE = "simple_example.spiders"
SCHEDULER = "scrapy_distributed.schedulers.DistributedScheduler"
SCHEDULER_QUEUE_CLASS = "scrapy_distributed.queues.amqp.RabbitQueue"
RABBITMQ_CONNECTION_PARAMETERS = "amqp://guest:guest@localhost:5672/?heartbeat=0"
DUPEFILTER_CLASS = "scrapy_distributed.dupefilters.redis_bloom.RedisBloomDupeFilter"
BLOOM_DUPEFILTER_REDIS_URL = "redis://:@localhost:6379/0"
BLOOM_DUPEFILTER_REDIS_HOST = "localhost"
BLOOM_DUPEFILTER_REDIS_PORT = 6379
REDIS_BLOOM_PARAMS = {"redis_cls": "redisbloom.client.Client"}
BLOOM_DUPEFILTER_ERROR_RATE = 0.001
BLOOM_DUPEFILTER_CAPACITY = 100_0000

# REDIRECT_ENABLED = False
# SCHEDULER_REQUEUE_ON_STATUS = ["302"]
# METAREFRESH_IGNORE_TAGS = ["delivery_tag"]

LOG_LEVEL = "INFO"
# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'simple_example (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'simple_example.middlewares.SimpleExampleSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": None,
    "simple_example.middlewares.SimpleExampleDownloaderMiddleware": 540,
    "scrapy_distributed.middlewares.amqp.RabbitMiddleware": 542
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'simple_example.pipelines.SimpleExamplePipeline': 300,
   'scrapy_distributed.pipelines.amqp.RabbitPipeline': 301,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
