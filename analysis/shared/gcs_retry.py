from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

from loguru import logger

GCS_RETRY_INITIAL_S = 1
GCS_RETRY_MAX_S = 30

T = TypeVar("T")


def next_gcs_retry_delay(current_seconds: int) -> int:
    return min(current_seconds * 2, GCS_RETRY_MAX_S)


def retry_gcs_call(
    operation: Callable[[], T],
    *,
    what: str,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    delay = GCS_RETRY_INITIAL_S
    while True:
        try:
            return operation()
        except Exception as ex:
            logger.error("Failed to {}: {} - retrying in {}s", what, ex, delay)
            sleep(delay)
            delay = next_gcs_retry_delay(delay)
