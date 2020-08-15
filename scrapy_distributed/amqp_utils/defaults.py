#!/usr/bin/env python
# -*- coding: utf-8 -*-
AMQP_ENCODING = "utf-8"
# Sane connection defaults.
AMQP_PARAMS = {
    "socket_timeout": 30,
    "socket_connect_timeout": 30,
    "retry_on_timeout": True,
    "encoding": AMQP_ENCODING,
}

SCHEDULER_KEY = "%(spider)s:scheduler"
PIPELINE_KEY = "%(spider)s:pipeline"
