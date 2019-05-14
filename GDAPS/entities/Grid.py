"""
This class is used to create the grid instance.
"""

from GDAPS.entities import Link
from GDAPS.entities import StorageElement
from GDAPS.entities import Protocol

from random import uniform, randint
import numpy as np



class Grid:

    def __init__(self, env, data_centers, ddm, wms, overhead):
        self.env = env
        for dc in data_centers:
            dc.grid = self
        self.data_centers = data_centers
        self.distributed_data_management_system = ddm
        self.distributed_data_management_system.grid = self
        self.workload_management_system = wms
        self.workload_management_system.grid = self
        self.running_jobs = []
        # this attribute keeps all the datasets,
        # which can be used to perform regression
        # key: "link-id;protocol"
        # value: respective dataset
        self.transfer_datasets = {}
        self.protocol = Protocol(overhead)

    def get_all_storage_elements(self):
        all_SEs = []
        for dc in self.data_centers:
            all_SEs += dc.storage_elements
        return all_SEs

    def get_all_worker_nodes(self):
        all_WNs = []
        for dc in self.data_centers:
            all_WNs += dc.worker_nodes
        return all_WNs

    # link only the subset of relevant machines
    def link_machines(self, all_replicas, jobs_dc, bandwidth, mean, std):
        self.links = {}
        # add all local SEs
        all_SEs = jobs_dc.storage_elements[:]
        # add all SEs with a replica
        for replica in all_replicas:
            if replica.storage_element not in all_SEs:
                all_SEs.append(replica.storage_element)
        all_WNs = jobs_dc.worker_nodes[:]

        # for later manipulations
        # save subsets of machines,
        # which are relevant to the current simulation
        self.SE_subset = all_SEs[:]
        self.WN_subset = all_WNs[:]

        for i in range(len(all_SEs)):
            u = all_SEs[i]
            for k in range(len(all_WNs)):
                v = all_WNs[k]
                if u.data_center == v.data_center:
                    # stage-in and remote data access
                    #bandwidth = uniform(Link.LOCAL_SE_TO_WN_MIN_BANDWIDTH, Link.LOCAL_SE_TO_WN_MAX_BANDWIDTH)
                    pass
                else:
                    # remote data access only
                    #bandwidth = uniform(Link.REMOTE_SE_TO_WN_MIN_BANDWIDTH, Link.REMOTE_SE_TO_WN_MAX_BANDWIDTH)
                    pass
                link = Link(u, v, self, self.env, bandwidth, mean, std)
                self.links[link.id] = link

            SE_subset = all_SEs[:]
            SE_subset.remove(all_SEs[i])
            for j in range(len(SE_subset)):
                v = SE_subset[j]
                # data placement only
                if u.data_center == v.data_center:
                    #bandwidth = uniform(Link.LOCAL_SE_TO_SE_MIN_BANDWIDTH, Link.LOCAL_SE_TO_SE_MAX_BANDWIDTH)
                    pass
                else:
                    #bandwidth = uniform(Link.REMOTE_SE_TO_SE_MIN_BANDWIDTH, Link.REMOTE_SE_TO_SE_MAX_BANDWIDTH)
                    pass
                link = Link(u, v, self, self.env, bandwidth, mean, std)
                self.links[link.id] = link

        self.distributed_data_management_system.set_transfer_quotas()

    def get_link(self, u, v):
        linkId = "{}-{}".format(u.id, v.id)
        return self.links[linkId]

    def update_background_load(self, link):
        link.background_load = max(0, int(np.random.normal(link.background_load_mean, link.background_load_std, 1).item()))
