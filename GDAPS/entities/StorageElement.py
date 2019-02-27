"""
This class is used to create storage element instances.
A storage element belongs to a single data center.
A storage element persists replicas.
"""

import uuid

from GDAPS.entities import Host
from GDAPS.entities import Replica



class StorageElement(Host):

    def __init__(self, capacity):
        Host.__init__(self)
        # capacity in MB
        self.capacity = float(capacity)

        # will be assigned, when a list of SEs is passed to a DC
        self.data_center = None

        self.id = uuid.uuid4().int
        # list of persisted replicas
        self.replicas = []
        self.available_capacity = self.capacity

    def replicate(self, file, reserved_space):
        if file.size > self.available_capacity and not reserved_space:
            print("NO SPACE.")
            return None
        replica = Replica(self, file)
        self.replicas.append(replica)
        file.replicated_at.append(self)
        file.replicas.append(replica)

        if not reserved_space:
            self.available_capacity -= file.size

        return replica

    def remove_replica(self, replica):
        self.replicas.remove(replica)
        self.available_capacity += replica.size

    def has_replica(self, file):
        for replica in self.replicas:
            if(replica.file == file):
                return True
        return False

    def __repr__(self):
        return 'Storage Element #{}, located at {}'.format(self.id, self.data_center)
