import logging
from scrapy.utils.misc import load_object

logger = logging.getLogger(__name__)


class RedisBloomSchedulerMixin(object):
    def init_redis_bloom(self, spider):
        logger.debug("RedisBloomSchedulerMixin, init_redis_bloom")
        dupefilter_cls = load_object(spider.settings["DUPEFILTER_CLASS"])
        self.df = dupefilter_cls.from_spider(spider)
