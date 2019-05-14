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

    def __init__(self, env, mips, job_slots, scratch_capacity):
        Host.__init__(self)

        self.env = env

        # per single job slot
        self.mips = int(mips)

        self.amount_jobs_slots = int(job_slots)
        self.job_slots = simpy.Resource(env, capacity=job_slots)
        self.assigned_jobs = []

        self.scratch_capacity = float(scratch_capacity)

        #will be assigned, when a list of WNs is passed to a DC
        self.data_center = None
        self.id = uuid.uuid4().int

    def stage_in(self, replica, job_id):

        link = self.data_center.grid.get_link(replica.storage_element, self)
        read = 0
        while read < replica.size:
            if self.scratch_capacity - replica.size >= 0:
                # reserve scratch space immediately
                self.scratch_capacity -= replica.size
                # read
                link.campaign_load += 1
                while read < replica.size:
                    # add a new transferred chunk
                    chunk = link.transfer_chunk(replica, "XRDCP", n_threads=1, job_id=job_id, read=read)
                    read += chunk
                    yield self.env.timeout(Time.SECOND)
                # release
                link.campaign_load -= 1
                self.scratch_capacity += replica.size
            else:
                print('Not enough scratch space to perform stage-in.')
                yield self.env.timeout(Time.STAGE_IN_WAITING_TIME)

    def execute_job(self, job):
        return math.ceil(job.workload/self.mips)
