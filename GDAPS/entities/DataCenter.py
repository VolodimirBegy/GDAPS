"""
This class is used to create data center instances.
"""

import uuid
from .StorageElement import StorageElement
from .WorkerNode import WorkerNode

from random import choice



class DataCenter:

    def __init__(self, storage_elements, worker_nodes):
        for se in storage_elements:
            if not isinstance(se, StorageElement):
                raise Exception('An object provided in the SE list is not of the appropriate type')
            se.data_center = self
        self.storage_elements = storage_elements

        for wn in worker_nodes:
            if not isinstance(wn, WorkerNode):
                raise Exception('An object provided in the WN list is not of the appropriate type')
            wn.data_center = self
        self.worker_nodes = worker_nodes

        self.id = uuid.uuid4().int

    def __repr__(self):
        return str(self.id)
