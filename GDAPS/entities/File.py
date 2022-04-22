"""
This class is used to create file instances.
A file is an abstract entity.
A replica is a realization of a file.
"""

import uuid



class File:

    def __init__(self, size, id):
        self.id = id#uuid.uuid4().int
        # size in MB
        self.size = float(size)
        # list of storage elements
        self.replicated_at = []
        self.replicas = []
