#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from scrapy_distributed.common.queue_config import KafkaQueueConfig
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread

default_serialize = ScrapyJSONEncoder().encode

logger = logging.getLogger(__name__)


