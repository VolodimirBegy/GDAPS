"""
This class is used to create replica instances.
A replica is a realization of a file.
A replica which is persisted at a single storage element.
"""

import uuid



class Replica:

    def __init__(self, storage_element, file):
        self.storage_element = storage_element
        self.data_center = storage_element.data_center
        self.file = file
        self.size = file.size
        self.id = uuid.uuid4().int
        # expressed in simulation ticks
        self.accessed_at = 0
