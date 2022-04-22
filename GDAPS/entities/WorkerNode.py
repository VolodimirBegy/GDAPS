"""
This class is used to create instances of worker nodes.
Computational jobs are submitted to worker nodes.
"""

from GDAPS.entities import Host
from GDAPS.util import Time

import uuid
import simpy
import math



class WorkerNode(Host):

    def __init__(self, env, mips, job_slots, scratch_capacity, id):
        Host.__init__(self)

        self.env = env

        # per single job slot
        self.mips = int(mips)

        # job_slots.count: active jobs
        # job_slots.capacity: job capacity
        self.job_slots = simpy.Resource(env, capacity=job_slots)
        self.assigned_jobs = []

        self.scratch_capacity = simpy.Container(env, float(scratch_capacity), float(scratch_capacity))

        #will be assigned, when a list of WNs is passed to a DC
        self.data_center = None
        #self.id = uuid.uuid4().int
        self.id = id

    def stage_in(self, replica, job):
        grid = self.data_center.grid
        link = grid.get_link(replica.storage_element, self)

        yield self.scratch_capacity.get(replica.size)

        transfer_id = uuid.uuid4().int
        link.campaign_load[transfer_id] = [replica.size]
        link.campaign_load_changed = True

        '''key = "{};{}".format(link.id, "XRDCP")
        if key not in grid.transfer_datasets:
            grid.transfer_datasets[key] = [] #"Timestamp,S,ConPr,T\n"
        start = self.env.now'''

        read = 0

        ret_val = [None, None]
        while(read < replica.size):
            ret_val = yield self.env.process(link.transfer_chunk(replica, "XRDCP", job_id=self.id, read=read,
                                                                 transfer_id=transfer_id, BW=ret_val[1]))
            read += ret_val[0]
            yield self.env.timeout(Time.SECOND)

        '''end = self.env.now
        T = end - start
        S = replica.size
        ConPr = 0
        for tick in range(start, end):
            chunks_dict = link.history[tick]
            for _, value in chunks_dict.items():
                ConPr += value
        # the above loop will redundantly add
        # the current replica. subtract its size
        ConPr -= replica.size'''
        #memory
        #grid.transfer_datasets[key].append([S, ConPr, T])

        del link.campaign_load[transfer_id]
        link.campaign_load_changed = True

    def execute_job(self, job):
        yield self.env.timeout(math.ceil(job.workload / self.mips))

    def release_scratch_space(self, size):
        # to avoid the error with fractional digits
        '''eps = self.scratch_capacity.level + size - self.scratch_capacity.capacity
        if eps > 0:
            print("eps {}".format(eps))
            yield self.scratch_capacity.put(self.scratch_capacity.capacity-self.scratch_capacity.level)
        else:
            yield self.scratch_capacity.put(size)'''
        yield self.scratch_capacity.put(size-0.0001)
        #alternative: halt all other processes. put. resume all other processes
        #WARNING: may yield errors if scratch capacity is unreasonably astronomical.
        #e.g. if capacity is sys.float.max (1.7976931348623157e+308)
        #initial capacity: 1.7976931348623157e+308
        #get 100000000000000:
        #1.7976931348623157e+308 - 100000000000000 == 1.7976931348623157e+308
        #True. thus, level still equals capacity after this get. and this put attempt will
        #yield an error (see simpy source: resources/container.py line 86)
