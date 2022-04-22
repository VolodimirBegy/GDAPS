"""
This class contains meta-information about time units, expressed in simulation ticks.
"""



class Time():

    SECOND = 1
    MINUTE = SECOND*60
    HOUR = MINUTE*60
    DAY = HOUR*24
    WEEK = DAY*7
    MONTH = DAY*31
    YEAR = MONTH*12
    CLEAN_UP_INTERVAL = MINUTE
    STAGE_IN_WAITING_TIME = MINUTE*10
