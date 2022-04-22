"""
This class is used to create job instances.
A job runs on a single worker node.
A job read the input data using one of the access profiles.
"""

from GDAPS.entities import AccessProfile

from GDAPS.util import Time

import uuid



class Job(object):

    def __init__(self, env, required_files, data_center):
        self.env = env
        self.id = uuid.uuid4().int
        self.required_files = required_files
        # data center, which will run the job
        self.data_center = data_center
        # will be assigned in WMS.submit()
        self.worker_node = None
        self.finished_data_placement = {}

        self.replicas = []
        self.access_profiles = []
        #todo
        self.workload = 10000

    def __repr__(self):
        return str(self.id)

    def assign_replicas(self, replicas):
        if len(replicas) != len(self.required_files):
            raise Exception('Not every required file has been assigned a replica.')

        for i in range(len(self.required_files)):
            if replicas[i].file != self.required_files[i]:
                raise Exception('Provided replica is assigned to a wrong file.')

        self.replicas = replicas

    def assign_access_profiles(self, access_profiles):

        if len(access_profiles) != len(self.replicas):
            raise Exception('Not every required replica has been assigned an access profile.')

        for i in range(len(self.replicas)):
            if ((self.replicas[i].storage_element.data_center != self.data_center and
               access_profiles[i] not in AccessProfile.REMOTE_DATA_ACCESS_PROFILES) or
               (self.replicas[i].storage_element.data_center == self.data_center and
               access_profiles[i] not in AccessProfile.LOCAL_DATA_ACCESS_PROFILES)):
                raise Exception('Inappropriate access profile assigned to a replica.')

        self.access_profiles = access_profiles

    def read_data(self, i):
        grid = self.data_center.grid
        worker_node = self.worker_node
        replica = self.replicas[i]
        # get the newly placed replica, if needed
        if self.access_profiles[i] in AccessProfile.INCLUDES_DATA_PLACEMENT:
            replica = self.finished_data_placement[i]

        # now either stage-in or read remotely
        link = grid.get_link(replica.storage_element, worker_node)

        if self.access_profiles[i] in AccessProfile.INCLUDES_STAGE_IN:
            # init() events always get the highest priority at any
            # given tick. so this is great logic-wise
            yield self.env.process(worker_node.stage_in(replica, self))

        elif self.access_profiles[i] in AccessProfile.INCLUDES_REMOTE_DATA_ACCESS:
            yield worker_node.scratch_capacity.get(replica.size)
            # remote data access
            transfer_id = self.id
            if transfer_id not in link.campaign_load:
                link.campaign_load[transfer_id] = [replica.size]
            else:
                link.campaign_load[transfer_id].append(replica.size)
            link.campaign_load_changed = True

            read = 0

            '''key = "{};{}".format(link.id, "WEBDAV")
            if key not in grid.transfer_datasets:
                grid.transfer_datasets[key] = []
            start = self.env.now'''

            ret_val = [None, None]
            while(read < replica.size):
                # add a new transferred chunk
                ret_val = yield self.env.process(link.transfer_chunk(replica, "WEBDAV", job_id=self.id, read=read,
                                                                     transfer_id=transfer_id, BW=ret_val[1]))
                read += ret_val[0]
                yield self.env.timeout(Time.SECOND)
                # init events get executed with highest priority
                # thus, even if a new transfer appears, while this original
                # transfer loops, the new transfer's load will be immediately
                # acknowledged

            #delet this
            '''end = self.env.now
            T = end - start
            S = replica.size
            ConTh, ConPr = 0, 0
            for tick in range(start, end):
                chunks_dict = link.history[tick]
                # key: job.id;protocol
                # value: MB
                for key2, value in chunks_dict.items():
                    job_id = key2.split(";")[0]
                    protocol = key2.split(";")[1]
                    if int(job_id) == self.id and protocol == "WEBDAV":
                        ConTh += value
                    else:
                        ConPr += value
            # the above loop will redundantly add
            # the current replica. subtract its size
            ConTh -= replica.size'''
            #memory
            #grid.transfer_datasets[key].append([S, ConTh, ConPr, T])
            #end delet this

            link.campaign_load[transfer_id].remove(replica.size)
            if len(link.campaign_load[transfer_id]) == 0:
                del link.campaign_load[transfer_id]
            link.campaign_load_changed = True
        else:
            raise Exception('Invalid data access behavior.')

    def run(self):
        total_file_size = 0
        for i in range(len(self.replicas)):
            total_file_size += self.replicas[i].size
        if total_file_size > self.worker_node.scratch_capacity.capacity:
            raise Exception('not enough scratch scpace to execute the job')

        read_requests = []
        for i in range(len(self.replicas)):
            read_requests.append(self.env.process(self.read_data(i)))
        yield from read_requests
        for rr in read_requests:
            if not rr.processed:
                raise Exception("yield from read_requests")

        yield self.env.process(self.worker_node.execute_job(self))
        yield self.env.process(self.worker_node.release_scratch_space(sum([r.size for r in self.replicas])))
        #self.worker_node.scratch_capacity.put(sum([r.size for r in self.replicas]))
        # process replicas based on mips?
