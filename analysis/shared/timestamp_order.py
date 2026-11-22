def is_backward_timestamp(timestamp_nano: int, last_accepted_nano: int | None) -> bool:
    return last_accepted_nano is not None and timestamp_nano < last_accepted_nano
