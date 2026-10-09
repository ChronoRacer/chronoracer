import pathway as pw


class DataStreamSchema(pw.Schema):
        sensor_id: str
        timestamp_nanos: int
        value: float
        driver_id: str
