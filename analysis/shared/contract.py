from pydantic import BaseModel, Field
from typing import Annotated

TimestampUs = Annotated[int, Field(ge=1767225600000000)]  # TIMESTAMP IN MICROSECONDS (LOWER BOUND = 01.01.2026 UTC)

ID = Annotated[str, Field(min_length=1)] # THIS IS WHAT ANALYSIS SENDS TO DASHBOARD TO IDENTIFY POSSIBLE PREDICTION, ALSO DASHBOARD CAN USE IT TO CACHE IT ETC

SensorId = Annotated[str,Field(min_length=1)]  # THIS IS WHAT I GET FROM CORE WHEN I AM CONNECTING BY GRPC - SENSOR ID

RefreshRate = Annotated[int, Field(ge=1)] # PREDICTIONS REFRESH RATE


# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
# THIS IS SENDING AFTER ANALYSIS GET KNOWLEDGE ABOUT WHAT PREDICTIONS CAN BE MADE
# BASED ON THESE I INFORM DASHBOARD WHAT IT CAN EXPECT FROM ANALYSIS (PREDICTIONS)
# PREDICTION_ID IT CAN BE USED FURTHER BY DASHBOARD
class PredictionRunningJSON(BaseModel):
    timestamp: TimestampUs # timestamp in microseconds
    prediction_id : ID # THIS IS ONE ID FOR ONE TYPE OF PREDICTION
    name : str = Field(...,min_length=1) # "XYZ PREDICTION" - dashboard can use it as a title (especially prediction title)
    refresh_rate: RefreshRate | None  # THIS CAN BE NONE BECAUSE THERE ARE PREDICTION WHERE YOU CAN'T DETERMINE REFRESH RATE.
    # WHERE WE CAN SAY EXACTLY TIME THERE THIS TIME WILL BE SEND

# THIS IS NEEDED WHEN PREDICTION CANT BE MADE BECAUSE OF
# E.G NOT RESPONDING ONE OF SENSOR THAT WE NEED TO FORMULA
# THEN ANALYSIS SENDS IT TO DASHBOARD

class PredictionUnavailableJSON(BaseModel):
    timestamp: TimestampUs  # timestamp in microseconds
    prediction_id: ID
    info : str = Field(...,min_length=1)

class SensorValue(BaseModel):
    sensor_id: SensorId
    value: float = Field(allow_inf_nan=False)

# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
# ONLY WHEN NEW ALERT HAS OCCURRED
class NewAlertJSON(BaseModel):
    timestamp : TimestampUs

    alert_id : ID #THIS IS ONE ID FOR ONE TYPE OF ALERT

    sensors : list[SensorValue] = Field(min_length=1)

    analysis_message : str = Field(...,min_length=1,max_length=255)
    # TEXT FROM RULE/S THAT WAS FIRED ON

    analysis_reason : str = Field(...,min_length=1,max_length=255)
    # this is a description with flag and definition of the flag
    # (this is constant i mean this is from definitions,
    # this is not changing between records i mean, definitions are the same
    # e.g RED-FLAG BECAUSE VALUE WAS HIGHER THAN BY ON WINDOW Z



# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
# THIS IS TRICKY ONE, BECAUSE WHAT IS PREDICTION, THIS IS NOT A VALUE EVERY TIME
class NewPredictionJSON(BaseModel):
    timestamp: TimestampUs

    prediction_id: ID

    sensors_id: list[SensorId]

    instruction : str  | None

    value:        float | None


# MESSAGE WRITTEN TO THE `event` KAFKA TOPIC (PRIVATE LISTENER, kafka_event_topic in platform.json).
# Key  = alert_id or prediction_id, uppercase compact (same convention as all other topics).
# Body = this model serialised as UTF-8 JSON.
class DriverEventMessage(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    # Rule name or prediction id — the car computer uses this as a display label.
    value: float | dict[str, float] | None = None
    # Numeric reading/prediction, or mapping {sensor_id: value} when multiple sensors participate in an alert.
    state: str | None = None
    # Alert severity: BLACK, RED, YELLOW, SENSOR_ERROR. None for prediction messages.
