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

    LOCAL_SE_TO_WN_MIN_BANDWIDTH = 100.
    LOCAL_SE_TO_WN_MAX_BANDWIDTH = 500.
    REMOTE_SE_TO_WN_MIN_BANDWIDTH = 50.
    REMOTE_SE_TO_WN_MAX_BANDWIDTH = 200.
    LOCAL_SE_TO_SE_MIN_BANDWIDTH = 300.
    LOCAL_SE_TO_SE_MAX_BANDWIDTH = 500.
    REMOTE_SE_TO_SE_MIN_BANDWIDTH = 1000.
    REMOTE_SE_TO_SE_MAX_BANDWIDTH = 3000.

    LOCAL_SE_TO_WN_MIN_BG_REQUESTS = 10
    LOCAL_SE_TO_WN_MAX_BG_REQUESTS = 100
    REMOTE_SE_TO_WN_MIN_BG_REQUESTS = 1
    REMOTE_SE_TO_WN_MAX_BG_REQUESTS = 10
    SE_TO_SE_MIN_BG_REQUESTS = 50
    SE_TO_SE_MAX_BG_REQUESTS = 500

    def __init__(self, u, v, grid, env, bandwidth, mean, std):
        self.env = env

        if isinstance(u, WorkerNode):
            raise Exception('Illegal link from a worker node.')
        if u == v:
            raise Exception('Illegal link from a machine to itself.')

        self.u = u
        self.v = v

        # MBps
        self.bandwidth = float(bandwidth)

        self.grid = grid

        if isinstance(u, StorageElement) and isinstance(v, StorageElement):
            lower_bound = self.SE_TO_SE_MIN_BG_REQUESTS
            upper_bound = self.SE_TO_SE_MAX_BG_REQUESTS
        elif u.data_center == v.data_center:
            lower_bound = self.LOCAL_SE_TO_WN_MIN_BG_REQUESTS
            upper_bound = self.LOCAL_SE_TO_WN_MAX_BG_REQUESTS
        else:
            lower_bound = self.REMOTE_SE_TO_WN_MIN_BG_REQUESTS
            upper_bound = self.REMOTE_SE_TO_WN_MAX_BG_REQUESTS
        # in amount of requests
        self.background_load_mean = mean
        self.background_load_std = std
        self.background_load = max(0, int(np.random.normal(mean, std, 1).item()))

        # cumulatively from any protocols
        # among these 2 hosts
        self.campaign_load = 0

        self.background_load_update_preiod = 180
        self.updated_load_counter = 0

        # used to create datasets
        self.history = {}
        self.id = "{}-{}".format(u.id, v.id)

    def transfer_chunk(self, remote_replica, protocol, n_threads, job_id, read):
        if protocol not in ['WEBDAV', 'GSIFTP', 'XRDCP']:
            # todo: also check validity based on self.type
            raise Exception('Invalid protocol.')

        remote_replica.accessed_at = self.env.now

        if int(self.env.now / self.background_load_update_preiod) > self.updated_load_counter:
            self.updated_load_counter = int(self.env.now / self.background_load_update_preiod)
            self.grid.update_background_load(self)

        transferred_MB = (self.bandwidth / (self.background_load + self.campaign_load)) / n_threads
        transferred_MB -= (transferred_MB * self.grid.protocol.overhead[protocol])
        # do not transfer a chunk which exceeds the filesize
        transferred_MB = min(transferred_MB, remote_replica.size - read)

        key = self.env.now
        if key not in self.history:
            self.history[key] = {}
        subkey = '{};{}'.format(job_id, protocol)
        if subkey not in self.history[key]:
            self.history[key][subkey] = transferred_MB
        else:
            self.history[key][subkey] += transferred_MB

        return transferred_MB
