#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis


REDIS_CLS = redis.StrictRedis
REDIS_ENCODING = "utf-8"
# Sane connection defaults.
REDIS_PARAMS = {
    "socket_timeout": 30,
    "socket_connect_timeout": 30,
    "retry_on_timeout": True,
    "encoding": REDIS_ENCODING,
}

SCHEDULER_DUPEFILTER_KEY = "%(spider)s:dupefilter"
DUPEFILTER_ERROR_RATE = 0.001
DUPEFILTER_CAPACITY = 100_0000
DUPEFILTER_EXCLUDE_URL_QUERY_PARAMS = True
