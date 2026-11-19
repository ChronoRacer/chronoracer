from pydantic import BaseModel, Field, field_validator
from typing import Annotated

TimestampUs = Annotated[int, Field(ge=1767225600000000)]  # TIMESTAMP IN MICROSECONDS (LOWER BOUND = 01.01.2026 UTC)

ID = Annotated[str, Field(min_length=1)] # THIS IS WHAT ANALYSIS SENDS TO DASHBOARD TO IDENTIFY POSSIBLE PREDICTION, ALSO DASHBOARD CAN USE IT TO CACHE IT ETC

SensorId = Annotated[str,Field(min_length=1)]  # THIS IS WHAT I GET FROM CORE WHEN I AM CONNECTING BY GRPC - SENSOR ID

RefreshRate = Annotated[str, Field(min_length=1)]  # Human-readable refresh cadence label for the dashboard.


# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
# THIS IS SENDING AFTER ANALYSIS GET KNOWLEDGE ABOUT WHAT PREDICTIONS CAN BE MADE
# BASED ON THESE I INFORM DASHBOARD WHAT IT CAN EXPECT FROM ANALYSIS (PREDICTIONS)
# PREDICTION_ID IT CAN BE USED FURTHER BY DASHBOARD
class PredictionRunningJSON(BaseModel):
    timestamp: TimestampUs # timestamp in microseconds
    prediction_id : ID # THIS IS ONE ID FOR ONE TYPE OF PREDICTION
    name : str = Field(...,min_length=1) # "XYZ PREDICTION" - dashboard can use it as a title (especially prediction title)
    refresh_rate: RefreshRate | None  # None when cadence cannot be determined; otherwise a display string (not a numeric Hz).
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

    value:        float | None = None

    @field_validator("value")
    @classmethod
    def round_value(cls, v: float | None) -> float | None:
        if v is not None:
            return round(float(v), 3)
        return None
