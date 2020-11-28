#!/usr/bin/env python
# -*- coding: utf-8 -*-

class IQueue(object):
    """Per-spider queue/stack base class"""

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, url):
        """Push an url"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop an url"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        raise NotImplementedError


__all__ = ["common", "amqp", "kafka"]
