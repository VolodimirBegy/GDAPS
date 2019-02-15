"""
This class contains meta-information about data access profiles.
"""



class AccessProfile():

    REMOTE_DATA_ACCESS = 1
    STAGE_IN = 2
    DATA_PLACEMENT_AND_STAGE_IN = 3
    DATA_PLACEMENT_AND_REMOTE_DATA_ACCESS = 4

    LOCAL_DATA_ACCESS_PROFILES = [REMOTE_DATA_ACCESS, STAGE_IN]
    REMOTE_DATA_ACCESS_PROFILES = [REMOTE_DATA_ACCESS, DATA_PLACEMENT_AND_STAGE_IN, \
                                   DATA_PLACEMENT_AND_REMOTE_DATA_ACCESS]

    INCLUDES_DATA_PLACEMENT = [DATA_PLACEMENT_AND_STAGE_IN, DATA_PLACEMENT_AND_REMOTE_DATA_ACCESS]
    INCLUDES_REMOTE_DATA_ACCESS = [REMOTE_DATA_ACCESS, DATA_PLACEMENT_AND_REMOTE_DATA_ACCESS]
    INCLUDES_STAGE_IN = [STAGE_IN, DATA_PLACEMENT_AND_STAGE_IN]
