"""
This class is used to create network link instances.
A link is virtual and uni-directional.
A link can be created between the following pairs of entities (src - dst):
1) Storage element - storage element
2) Storage element - worker node
"""

from GDAPS.entities import StorageElement
from GDAPS.entities import WorkerNode
from GDAPS.entities import AccessProfile
from GDAPS.entities import Protocol

from random import randint
import numpy as np



class Link:

    def __init__(self, u, v, grid, env, bandwidth, background_load_range, background_load_t, size_weighted_factor, throttle):
        self.env = env

        if isinstance(u, WorkerNode):
            raise Exception('Illegal link from a worker node.')
        if u is v:
            raise Exception('Illegal link from a machine to itself.')

        self.u = u
        self.v = v
        self.grid = grid
        # cumulatively from any protocols
        # among these 2 hosts
        self.campaign_load = {}

        self.bandwidth = bandwidth
        self.background_load_range = background_load_range #e.g. [100, 20, 30, 10] etc, pick cyclically
        self.background_load_t = background_load_t
        self.background_load = self.background_load_range[int(self.env.now/self.background_load_t)%len(self.background_load_range)]
        self.size_weighted_factor = size_weighted_factor
        self.throttle = throttle
        #
        self.background_load_updated = env.now

        # used to create datasets
        # memory
        #self.history = {}
        self.id = "{}-{}".format(u.id, v.id)
        #self.bw = {}

    # all remote data access threads for a job are executed within 1 same process
    # each stage-in access is executed by a new copytool process
    # each data-placement access is executed by a new DDM process
    # job_id is used to assign the webdav traffic either to ConTh or ConPr
    # transfer_id is used to weigh the bandwidth allocated to a process
    # job_id is not enough for the last task, because a single job may have different
    # copytool processes for stage-in
    def transfer_chunk(self, remote_replica, protocol, job_id, read, transfer_id=None, BW=None):
        yield self.env.timeout(-1)
        '''for event in self.env._queue:
            if event[0] < self.env.now or (event[1] < 2 and event[0] <= self.env.now):
                raise Exception("priority error")'''

        if protocol not in ['WEBDAV', 'GSIFTP', 'XRDCP']:
            # todo: also check validity based on self.type
            raise Exception('Invalid protocol.')

        remote_replica.accessed_at = self.env.now

        recalc_BW = False
        if self.background_load_updated + int(self.background_load_t * 60) == self.env.now:
            self.background_load = self.background_load_range[int(self.env.now/self.background_load_t)%len(self.background_load_range)]
            recalc_BW = True
            yield self.env.process(self.update_sync1())
            self.background_load_updated = self.env.now

        if BW is None or recalc_BW or self.campaign_load_changed:
            yield self.env.process(self.bw_calc_sync())
            self.campaign_load_changed = False

            campaign_bandwidth_ratio = (float(len(self.campaign_load))
                                       / (self.background_load + len(self.campaign_load)))
            campaign_bandwidth = self.bandwidth * campaign_bandwidth_ratio
            total_size_of_current_transfers = 0
            for t_id in self.campaign_load:
                total_size_of_current_transfers += sum(self.campaign_load[t_id])

            #total_size_of_current_transfers += len(self.campaign_load)
            inverse_weights = []
            current_weight = None
            for t_id in self.campaign_load:
                inverse_weights.append(1. / (float(sum(self.campaign_load[t_id]))
                                             / total_size_of_current_transfers))
                if t_id == transfer_id:
                    current_weight = inverse_weights[-1]

            process_bandwidth_ratio = current_weight / sum(inverse_weights)
            process_bandwidth = campaign_bandwidth * process_bandwidth_ratio

            process_bandwidth_unweighted = self.bandwidth / (self.background_load + len(self.campaign_load))

            BW = (process_bandwidth_unweighted - self.size_weighted_factor
                 * (process_bandwidth_unweighted - process_bandwidth))


        '''if self.env.now in self.bw:
            if self.bw[self.env.now] != BW:
                raise Exception('weird shit happening')
        else:
            self.bw[self.env.now] = BW'''
        n_threads = len(self.campaign_load[transfer_id])
        #print("tick {} | BW {}".format(self.env.now, BW / n_threads))
        transferred_MB = min(self.throttle, BW / n_threads)

        transferred_MB -= (transferred_MB * self.grid.protocol.overhead[protocol])
        # do not transfer a chunk which exceeds the filesize
        transferred_MB = min(transferred_MB, remote_replica.size - read)

        '''key = self.env.now
        if key not in self.history:
            self.history[key] = {}
        subkey = '{};{}'.format(job_id, protocol)
        if subkey not in self.history[key]:
            self.history[key][subkey] = transferred_MB
        else:
            self.history[key][subkey] += transferred_MB'''
        #print(transferred_MB)
        return [transferred_MB, BW]

    def update_sync1(self):
        yield self.env.timeout(-2)
        '''for event in self.env._queue:
            if event[0] < self.env.now or (event[1] < 3 and event[0] <= self.env.now):
                raise Exception("priority error")'''

    def bw_calc_sync(self):
        yield self.env.timeout(-4)
        '''for event in self.env._queue:
            if event[0] < self.env.now or (event[1] < 5 and event[0] <= self.env.now):
                print(self.env._queue)
                raise Exception("priority error")'''
