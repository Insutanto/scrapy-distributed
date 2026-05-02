import json
import pytest
from types import SimpleNamespace

from scrapy_distributed.queues.common import BytesDump, get_method, keys_string


class TestBytesDump:
    def test_encodes_bytes_value_to_string(self):
        data = {"key": b"hello"}
        result = json.dumps(data, cls=BytesDump)
        assert result == '{"key": "hello"}'

    def test_leaves_normal_string_unchanged(self):
        data = {"key": "hello"}
        result = json.dumps(data, cls=BytesDump)
        assert json.loads(result)["key"] == "hello"

    def test_non_serialisable_non_bytes_raises(self):
        with pytest.raises(TypeError):
            json.dumps({"key": object()}, cls=BytesDump)


class TestKeysString:
    def test_dict_with_bytes_keys(self):
        result = keys_string({b"foo": "bar", b"baz": 1})
        assert result == {"foo": "bar", "baz": 1}

    def test_dict_with_string_keys_unchanged(self):
        d = {"a": 1, "b": "two"}
        assert keys_string(d) == d

    def test_nested_dict(self):
        d = {b"outer": {b"inner": "value"}}
        assert keys_string(d) == {"outer": {"inner": "value"}}

    def test_dict_with_list_values(self):
        d = {"items": [b"a", b"b"]}
        result = keys_string(d)
        # list elements are processed recursively; bytes are returned as-is
        # (only dict keys are decoded, not list scalar values)
        assert result["items"] == [b"a", b"b"]

    def test_dict_with_nested_list_of_dicts(self):
        d = {"rows": [{b"k": "v"}]}
        result = keys_string(d)
        assert result == {"rows": [{"k": "v"}]}

    def test_non_dict_scalar_returned_as_is(self):
        assert keys_string("hello") == "hello"
        assert keys_string(42) == 42
        assert keys_string(None) is None

    def test_list_input_processed_element_wise(self):
        result = keys_string([{b"x": 1}, {b"y": 2}])
        assert result == [{"x": 1}, {"y": 2}]

    def test_tuple_input_processed_element_wise(self):
        result = keys_string(({b"a": 1},))
        assert result == [{"a": 1}]


class TestGetMethod:
    def test_returns_existing_attribute(self):
        obj = SimpleNamespace(parse=lambda: "ok")
        method = get_method(obj, "parse")
        assert method() == "ok"

    def test_missing_attribute_raises_value_error(self):
        obj = SimpleNamespace()
        with pytest.raises(ValueError, match="'missing'"):
            get_method(obj, "missing")

    def test_name_is_coerced_to_string(self):
        obj = SimpleNamespace()
        setattr(obj, "42", "found")
        assert get_method(obj, 42) == "found"
