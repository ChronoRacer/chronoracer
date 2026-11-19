from analysis.shared.connector import is_backward_timestamp


def test_first_point_is_never_backward():
    assert is_backward_timestamp(1_000, None) is False


def test_same_timestamp_is_not_backward():
    assert is_backward_timestamp(1_000, 1_000) is False


def test_later_timestamp_is_not_backward():
    assert is_backward_timestamp(1_100, 1_000) is False


def test_earlier_timestamp_is_backward():
    assert is_backward_timestamp(900, 1_000) is True
