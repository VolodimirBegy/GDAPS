"""
This class is used to create an instance of distributed data management system.
The DDM system performs data placement according to link quotas.
The DDM system removes outdated replicas from the local data center.
"""

from GDAPS.entities import StorageElement
from GDAPS.entities import AccessProfile

from GDAPS.util import Time

from random import randint
import simpy



class DistributedDataManagementSystem:

    def __init__(self, env):
        self.env = env
        self.grid = None
        self.transfer_quotas = {}

    def set_transfer_quotas(self):
        # only for data placement
        # among 2 storage elements
        links = self.grid.links
        for key, value in links.items():
            if isinstance(value.u, StorageElement) and isinstance(value.v, StorageElement):
                self.transfer_quotas[key] = simpy.Resource(self.env, capacity=randint(500, 2000))

    def request_data_placement(self, job, i):
        grid = self.grid
        # request to transfer the replica
        # to the first available local SE
        remote_replica = job.replicas[i]
        local_SE = None

        while local_SE is None:
            for SE in job.data_center.storage_elements:
                if SE.available_capacity >= remote_replica.size:
                    local_SE = SE
                    # reserve space
                    SE.available_capacity -= remote_replica.size
                    break

            if local_SE is None:
                # check, whether space became available
                # eventually it will become available
                yield self.env.timeout(Time.SECOND)
                #print('No space to perform data placement at{}.'.format(self.env.now))
                #print('Waiting for a day.')

        link = grid.get_link(remote_replica.storage_element, local_SE)
        with self.transfer_quotas[link.id].request() as req:
            yield req
            # at this point DDM can transfer the file at the given link
            #replicate
            link.campaign_load += 1
            read = 0

            key = "{};{}".format(link.id, "GSIFTP")
            if key not in grid.transfer_datasets:
                grid.transfer_datasets[key] = "Timestamp,S,ConPr,T\n"
            start = self.env.now

            while(read < remote_replica.size):
                chunk = link.transfer_chunk(remote_replica, "GSIFTP", n_threads=1, job_id=job.id, read=read)
                read += chunk
                yield self.env.timeout(Time.SECOND)

            end = self.env.now
            T = end- start
            S = remote_replica.size
            ConPr = 0
            for tick in range(start, end):
                chunks_dict = link.history[tick]
                for _, value in chunks_dict.items():
                    ConPr += value
            # the above loop will redundantly add
            # the current replica. subtract its size
            ConPr -= remote_replica.size
            grid.transfer_datasets[key] += "{},{},{},{}\n".format(start, S, ConPr, T)

            link.campaign_load -= 1
            local_replica = local_SE.replicate(file=remote_replica.file, reserved_space=True)
            # key -> value: index of replica -> local replica
            job.finished_data_placement[i] = local_replica
            #print('Successfully performed data-placement.')

    def clean_up(self):
        # once per hour (3600 ticks) perform the check:
        # if replica is not required by the campaign
        # and has not been touched for a day
        local_SEs = self.grid.running_jobs[0].data_center.storage_elements
        while len(self.grid.running_jobs) > 0:
            required_replicas = []
            for job in self.grid.running_jobs:
                required_replicas += job.replicas[:]

            for SE in local_SEs:
                for replica in SE.replicas:
                    if self.env.now - replica.accessed_at > Time.DAY and \
                    replica not in required_replicas:
                        SE.remove_replica(replica)
                        print("REMOVED REPLICA.")
            yield self.env.timeout(Time.CLEAN_UP_INTERVAL)
