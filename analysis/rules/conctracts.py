from datetime import datetime
from pydantic import BaseModel

# THIS IS SCHEMA FOR JSON DATA SENT FROM ANALYSIS TO DASHBOARD
# THIS IS SENDING AFTER ANALYSIS GET LIST OF SENSORS AND THEN
# BASED ON THESE I INFORM DASHBOARD WHAT IT CAN EXPECT FROM ANALYSIS (ALERTS AND PREDICTIONS)
# ALSO THIS IS IMPORTANT TO SET ID TO EVERY ALERT AND PREDICTION SO DASHBOARD CAN KNOW WHAT TO EXPECT FROM ANALYSIS
# E.G TO CHANGE FREQ DURING RACE

class AnalysisToDashboardInit(BaseModel):
    timestamp: datetime # datetime in ISO 8601 format

    refresh_rate: int | None # ALERTS DON'T HAVE REFRESH RATE BUT PREDICTIONS DO HAVE
    # FIRST SEND IS WITH DEFAULT VALUE

    alert_pred_id: int

class NewChangesFromAnalysisToDashboard(BaseModel):
    timestamp: datetime # datetime in ISO 8601 format

    sensor_list: list[str] # list of sensors to identify sensors between modules, these id are from core module
    # i believe i get these list when i am connecting to core module

    notification_kind: str # alert, prediction, flag-x (x is color)

    flag: str

    additional_value: float | None # additional value for prediction or alert, maybe value that break the rule??
"""
analysis -> dashboard
{

    timestamp[us] (ob),

    sensor_list / id sth to identitfy sensors between modules -
    i get list of sensors, so i can pass it further (ob)

notification_kind[alert, prediction,flag-x (x is color)] (ob)

additional_value

}
"""