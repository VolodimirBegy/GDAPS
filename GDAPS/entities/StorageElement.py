"""
This class is used to create storage element instances.
A storage element belongs to a single data center.
A storage element persists replicas.
"""

import uuid

from GDAPS.entities import Host
from GDAPS.entities import Replica

from simpy.resources.container import Container



class StorageElement(Host):

    def __init__(self, env, capacity, id):
        Host.__init__(self)
        self.env = env
        # capacity in MB
        self.capacity = Container(env, float(capacity), float(capacity))

        # will be assigned, when a list of SEs is passed to a DC
        self.data_center = None

        self.id = id#uuid.uuid4().int
        # list of persisted replicas
        self.replicas = []

    def replicate(self, file):
        yield self.capacity.get(file.size)

        replica = Replica(self, file)
        self.replicas.append(replica)
        file.replicated_at.append(self)
        file.replicas.append(replica)

        return replica

    def remove_replica(self, replica):
        if replica in self.replicas:
            self.replicas.remove(replica)
            replica.file.replicas.remove(replica)
            replica.file.replicated_at.remove(self)
            yield self.capacity.put(replica.size)
        else:
            return

    def has_replica(self, file):
        for replica in self.replicas:
            if(replica.file is file):
                return True
        return False

    def __repr__(self):
        return 'Storage Element #{}, located at {}'.format(self.id, self.data_center)
