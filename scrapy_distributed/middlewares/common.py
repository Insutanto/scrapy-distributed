#!/usr/bin/env python
# -*- coding: utf-8 -*-

def is_a_picture(response):
    picture_exts = [".png", ".jpg", ".jpeg", ".gif"]
    lower_url = response.url.lower()
    return any([lower_url.endswith(ext) for ext in picture_exts])
