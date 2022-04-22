"""
This class is used to create the grid instance.
"""

from GDAPS.entities import Link
from GDAPS.entities import StorageElement
from GDAPS.entities import Protocol

from random import uniform, randint
import numpy as np
import simpy
import ast



class Grid:

    def __init__(self, env, data_centers, ddm, wms, overhead):
        self.env = env
        for dc in data_centers:
            dc.grid = self
        self.data_centers = data_centers
        self.ddm = ddm
        self.ddm.grid = self
        self.workload_management_system = wms
        self.workload_management_system.grid = self
        self.running_jobs = []
        # this attribute keeps all the datasets,
        # which can be used to perform regression
        # key: "link-id;protocol"
        # value: respective dataset
        self.transfer_datasets = {}
        self.protocol = Protocol(overhead)
        self.links = {}

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
    def link_machines(self, all_replicas, jobs_dc, dp_transfer_quota):
        setup = "links\n"
        self.links = {}
        # add all local SEs
        local_SEs = jobs_dc.storage_elements[:]
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

                bandwidth = np.random.uniform(750, 1805) if u.data_center is v.data_center else np.random.uniform(125, 12500)
                background_load_range = [randint(10, 200) for _ in range(10)]
                background_load_t = randint(10, 60)
                size_weighted_factor = np.random.uniform(0, 1)
                throttle = np.random.uniform(5, 90)
                link = Link(u, v, self, self.env, bandwidth, background_load_range,
                            background_load_t, size_weighted_factor, throttle)
                setup += "{} {} {} {} {} {} {}\n".format(u.id, v.id, bandwidth, background_load_range,
                                                         background_load_t, size_weighted_factor, throttle)
                self.links[link.id] = link

            SE_subset = local_SEs[:]
            if all_SEs[i] in SE_subset:
                SE_subset.remove(all_SEs[i])
            for j in range(len(SE_subset)):
                v = SE_subset[j]

                bandwidth = np.random.uniform(750, 1805) if u.data_center is v.data_center else np.random.uniform(125, 12500)
                background_load_range = [randint(10, 200) for _ in range(10)]
                background_load_t = randint(10, 60)
                size_weighted_factor = np.random.uniform(0, 1)
                throttle = np.random.uniform(5, 90)
                link = Link(u, v, self, self.env, bandwidth, background_load_range,
                            background_load_t, size_weighted_factor, throttle)
                setup += "{} {} {} {} {} {} {}\n".format(u.id, v.id, bandwidth, background_load_range,
                                                         background_load_t, size_weighted_factor, throttle)
                self.links[link.id] = link

        self.ddm.set_transfer_quotas(dp_transfer_quota)
        return setup

    def link_machines_from_setup(self, setup, all_SEs, all_WNs, dp_transfer_quota):
        self.links = {}
        setup_index = 0
        while not setup[setup_index].startswith("job"):
            link_data = setup[setup_index].split("[")
            link_data1 = link_data[0].split(" ")[:-1]
            u, v = None, None
            if link_data1[0].startswith("SE"):
                u = all_SEs[int(link_data1[0].split("SE")[1])]
            else:
                u = all_WNs[int(link_data1[0].split("WN")[1])]
            if link_data1[1].startswith("SE"):
                v = all_SEs[int(link_data1[1].split("SE")[1])]
            else:
                v = all_WNs[int(link_data1[1].split("WN")[1])]
            bandwidth = float(link_data1[-1])
            link_data2 = link_data[1].split("]")
            background_load_range = ast.literal_eval("[" + link_data2[0] + "]")
            link_data3 = link_data2[1][1:].split(" ")
            background_load_t = int(link_data3[0])
            size_weighted_factor = float(link_data3[1])
            throttle = float(link_data3[2])
            link = Link(u, v, self, self.env, bandwidth, background_load_range,
                        background_load_t, size_weighted_factor, throttle)
            self.links[link.id] = link
            setup_index += 1

        self.ddm.set_transfer_quotas(dp_transfer_quota)
        return setup[setup_index:]


    def get_link(self, u, v):
        linkId = "{}-{}".format(u.id, v.id)

        if linkId not in self.links:
            link = Link(u, v, self, self.env)
            self.links[link.id] = link
            if isinstance(u, StorageElement) and isinstance(v, StorageElement):
                self.ddm.transfer_quotas[linkId] = simpy.Resource(self.env, capacity=100000)

        return self.links[linkId]
