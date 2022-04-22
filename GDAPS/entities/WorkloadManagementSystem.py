"""
This class is used to create instances of workload management systems.
"""

from GDAPS.entities import AccessProfile

from GDAPS.util import Time



class WorkloadManagementSystem:

    def __init__(self, env):
        self.env = env
        self.grid = None
        self.job_queues = {}

    def submit(self, job, dc):
        #print('Job {} submitted at {}.'.format(job, self.env.now))
        dc.grid.running_jobs.append(job)

        # submit to the WN with the lowest load
        wn = dc.worker_nodes[0]
        for tmp_wn in dc.worker_nodes:
            if len(tmp_wn.assigned_jobs) < len(wn.assigned_jobs):
                wn = tmp_wn

        job.worker_node = wn
        wn.assigned_jobs.append(job)

        data_placement_requests = []
        for i in range(len(job.access_profiles)):
            if job.access_profiles[i] in AccessProfile.INCLUDES_DATA_PLACEMENT:
                data_placement_request = self.env.process(self.grid.ddm.request_data_placement(job, i))
                data_placement_requests.append(data_placement_request)
        # wait for all data placement requests to terminate
        #print("{} data placement requests".format(len(data_placement_requests)))
        yield from data_placement_requests
        for dpr in data_placement_requests:
            if not dpr.processed:
                raise Exception("yield from data_placement_requests")

        with wn.job_slots.request() as req:
            yield req
            # at this point the job slot
            # became available:
            yield self.env.process(job.run())

        wn.assigned_jobs.remove(job)
        self.grid.running_jobs.remove(job)
        #print('Job {} finished at {}.'.format(job, self.env.now))
        print("{} jobs running".format(len(self.grid.running_jobs)))
        if(len(self.grid.running_jobs) == 0):
            print(self.env.now)
