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

def test_is_a_picture_with_gif_url():
    resp = DummyResponse(url='http://example.com/animation.gif')
    assert is_a_picture(resp)

def test_is_a_picture_case_insensitive():
    for url in ['http://example.com/a.GIF', 'http://example.com/a.PNG']:
        assert is_a_picture(DummyResponse(url=url))

def test_is_a_picture_with_non_image_urls():
    urls = [
        'http://example.com/index.html',
        'http://example.com/video.gifv',
        'http://example.com/photo.jpeg?download=true'
    ]
    for url in urls:
        resp = DummyResponse(url=url)
        assert not is_a_picture(resp)

def test_is_a_picture_query_string_breaks_extension_match():
    # The current implementation uses str.endswith(), so a query string after
    # the extension causes the URL to not be recognised as a picture.
    # This is existing/documented behaviour; it is tested here so that any
    # future change to handle query strings is made deliberately.
    for url in [
        'http://example.com/image.png?w=100',
        'http://example.com/photo.jpg?v=1',
    ]:
        assert not is_a_picture(DummyResponse(url=url))
