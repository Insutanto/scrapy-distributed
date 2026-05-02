import logging

from scrapy_distributed.schedulers.common_dupefilter import DupeFilterSchedulerMixin

logger = logging.getLogger(__name__)


class RedisBloomSchedulerMixin(DupeFilterSchedulerMixin):
    """Scheduler mixin for RedisBloom-based DupeFilters.

    .. deprecated:: 0.1.0
        Use :class:`~scrapy_distributed.schedulers.common_dupefilter.DupeFilterSchedulerMixin`
        directly instead.  This class is kept for backward compatibility and
        simply exposes ``init_redis_bloom`` as an alias for
        ``init_dupefilter``.
    """

    def init_redis_bloom(self, spider):
        """Alias for :meth:`init_dupefilter` kept for backward compatibility."""
        logger.debug("RedisBloomSchedulerMixin, init_redis_bloom")
        self.init_dupefilter(spider)

