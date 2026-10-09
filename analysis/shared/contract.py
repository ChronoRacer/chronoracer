from pydantic import BaseModel, Field
from typing import Annotated

TimestampUs = Annotated[int, Field(ge=1767225600000000)]  # TIMESTAMP IN MICROSECONDS (LOWER BOUND = 01.01.2026 UTC)

ID = Annotated[str, Field(min_length=1)] # THIS IS WHAT ANALYSIS SENDS TO DASHBOARD TO IDENTIFY POSSIBLE PREDICTION, ALSO DASHBOARD CAN USE IT  TO CHANGE E.G EVALUATION FREQUENCY

SensorId = Annotated[str,Field(min_length=1)]  # THIS IS WHAT I GET FROM CORE WHEN I AM CONNECTING BY GRPC - SENSOR ID

RefreshRate = Annotated[int, Field(ge=1)] # PREDICTIONS  REFRESH RATE


# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
# THIS IS SENDING AFTER ANALYSIS GET KNOWLEDGE ABOUT WHAT PREDICTIONS CAN BE MADE
# BASED ON THESE I INFORM DASHBOARD WHAT IT CAN EXPECT FROM ANALYSIS (PREDICTIONS)
# PREDICTION_ID IT CAN BE USED FURTHER BY DASHBOARD
# E.G TO CHANGE VALUATION FREQ DURING RACE -  FIRST TIME ANALYSIS SENDS IT WITH DEFAULT VALUE
class PredictionRunningJSON(BaseModel):
    timestamp: TimestampUs # timestamp in microseconds
    prediction_id : ID # THIS IS ONE ID FOR ONE TYPE OF PREDICTION
    name : str = Field(...,min_length=1) # "XYZ PREDICTION" - dashboard can use it as a title (especially prediction title)
    refresh_rate: RefreshRate

# THIS IS NEEDED WHEN PREDICTION CANT BE MADE BECAUSE OF
# E.G NOT RESPONDING ONE OF SENSOR THAT WE NEED TO FORMULA
# THEN ANALYSIS SENDS IT TO DASHBOARD

class PredictionUnavailableJSON(BaseModel):
    timestamp: TimestampUs  # timestamp in microseconds
    prediction_id: ID
    info : str = Field(...,min_length=1)


# THIS IS SCHEMA FOR JSON DATA SENT FROM DASHBOARD TO ANALYSIS
# 200 OK FROM ANALYSIS -> THAT MEANS CHANGE WAS DONE,
# WHEN DASHBOARD SENDS REQUEST WITH WRONG VALUE (freq or prediction_id) IT WILL BE ERROR CODE 4XX
class DashboardChangePredFreqJSON(BaseModel):
    timestamp: TimestampUs # timestamp in microseconds
    prediction_id: ID
    new_refresh_rate: RefreshRate # This is value in seconds, this is how often dashboard will expect new prediction from analysis module




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
# SENDING EVERY REFRESH_RATE (s) TIME
# THIS IS TRICKY ONE, BECAUSE WHAT IS PREDICTION, THIS IS NOT A VALUE EVERY TIME
class NewPredictionJSON(BaseModel):
    timestamp: TimestampUs

    prediction_id: ID

    sensors_id: list[SensorId]

    instruction : str  | None
    value:        float | None


"""
# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
class NewAnalysisJSON(BaseModel):
    timestamp: int = Field(...,ge=1767225600000000) # timestamp in microseconds

    alert_pred_id: int = Field(...,ge=1)

    sensor_list: list[str]
    # list of sensors to identify sensors between modules, these id are from core module
    # i believe i get these list when i am connecting to core module

    analysis_kind : int  = Field(...,min_length=1,pattern="^[a-zA-Z0-9_-]+$") # ALERT, PREDICTION,

    analysis_message: str | None = Field(None,min_length=1,max_length=127,pattern="^[a-zA-Z0-9_-]+$")
    # this is a message with id of sensor so you can map it to their name?
    # but this message is only  with alerts, because predictions there is no reason there
    # because reason are sending periodically

    analysis_reason: str
    # this is a description with flag and definition of the flag
    # (this is constant i mean this is from definitions,
    # this is not changing between records i mean, definitions are the same
    # e.g RED-FLAG BECAUSE VALUE WAS HIGHER THAN BY ON WINDOW Z

    value: float | None # additional value for prediction or alert, maybe value that break the rule??

"""

