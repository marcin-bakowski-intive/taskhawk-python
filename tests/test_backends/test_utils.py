import pytest

from taskhawk import Priority
from taskhawk.backends.utils import get_queue_name, import_class


@pytest.mark.parametrize(
    "priority, expected_queue_name",
    [
        (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
        (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
        (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
        (Priority.default, "TASKHAWK-DEV-RTEP"),
    ],
)
def test_get_queue_name(priority, expected_queue_name):
    assert get_queue_name(priority) == expected_queue_name


class TestImportClass:
    def test_success(self):
        assert import_class("tests.test_backends.test_utils.TestImportClass") is TestImportClass

    @pytest.mark.parametrize("dotted_path", [None, "", "invalid", "tests.test_backends.test_utils.NoClass"])
    def test_failure(self, dotted_path):
        with pytest.raises(ImportError):
            import_class(dotted_path)
