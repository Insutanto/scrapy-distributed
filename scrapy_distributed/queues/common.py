#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json


class BytesDump(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode()
        return json.JSONEncoder.default(self, obj)


def keys_string(d):
    rval = {}
    if not isinstance(d, dict):
        if isinstance(d, (tuple, list, set)):
            v = [keys_string(x) for x in d]
            return v
        else:
            return d

    for k, v in d.items():
        if isinstance(k, bytes):
            k = k.decode()
        if isinstance(v, dict):
            v = keys_string(v)
        elif isinstance(v, (tuple, list, set)):
            v = [keys_string(x) for x in v]
        rval[k] = v
    return rval


def get_method(obj, name):
    """Helper function for request_from_dict"""
    name = str(name)
    try:
        return getattr(obj, name)
    except AttributeError:
        raise ValueError(f"Method {name!r} not found in: {obj}")
