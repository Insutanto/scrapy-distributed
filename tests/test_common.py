import pytest
from types import SimpleNamespace

from scrapy_distributed.middlewares.common import is_a_picture

class DummyResponse(SimpleNamespace):
    pass

def test_is_a_picture_with_image_urls():
    urls = [
        'http://example.com/image.png',
        'http://example.com/sub/PHOTO.JPG',
        'http://example.com/picture.jpeg'
    ]
    for url in urls:
        resp = DummyResponse(url=url)
        assert is_a_picture(resp)

def test_is_a_picture_with_non_image_urls():
    urls = [
        'http://example.com/index.html',
        'http://example.com/video.gifv',
        'http://example.com/photo.jpeg?download=true'
    ]
    for url in urls:
        resp = DummyResponse(url=url)
        assert not is_a_picture(resp)
