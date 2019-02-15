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
        self.finished_data_placement = {}
        self.remote_data_access_threads = 0

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
            if (self.replicas[i].storage_element.data_center != self.data_center and \
                access_profiles[i] not in AccessProfile.REMOTE_DATA_ACCESS_PROFILES) or \
                (self.replicas[i].storage_element.data_center == self.data_center and \
                access_profiles[i] not in AccessProfile.LOCAL_DATA_ACCESS_PROFILES):
                    raise Exception('Inappropriate access profile assigned to a replica.')

        self.access_profiles = access_profiles

    def read_data(self, i):
        grid = self.data_center.grid
        replica = self.replicas[i]
        # get the newly placed replica, if needed
        if self.access_profiles[i] in AccessProfile.INCLUDES_DATA_PLACEMENT:
            replica = self.finished_data_placement[i]

        # now either stage-in or read remotely
        link = grid.get_link(replica.storage_element, self.worker_node)

        if self.access_profiles[i] in AccessProfile.INCLUDES_STAGE_IN:
            #delet this
            key = "{};{}".format(link.id, "XRDCP")
            if key not in grid.transfer_datasets:
                grid.transfer_datasets[key] = "Timestamp,S,ConPr,T\n"
            start = self.env.now
            #end delet this

            # stage-in
            # for stage-in every file access is handles by an individual process
            link.v.stage_in(replica, self.id)

            #delet this
            end = self.env.now
            T = end - start
            S = replica.size
            ConPr = 0
            for tick in range(start, end):
                chunks_dict = link.history[tick]
                # key: job.id;protocol
                # value: MB
                for _, value in chunks_dict.items():
                    ConPr += value
            # the above loop will redundantly
            # add the current replica. subtract it
            ConPr -= replica.size
            grid.transfer_datasets[key] += "{},{},{},{}\n".format(start, S, ConPr, T)
            #end delet this

        elif self.access_profiles[i] in AccessProfile.INCLUDES_REMOTE_DATA_ACCESS:
            # remote data access
            if self.remote_data_access_threads == 0:
                link.campaign_load += 1
            self.remote_data_access_threads += 1

            read = 0

            key = "{};{}".format(link.id, "WEBDAV")
            if key not in grid.transfer_datasets:
                grid.transfer_datasets[key] = []
            start = self.env.now

            while(read < replica.size):
                # add a new transferred chunk
                n_threads = self.remote_data_access_threads
                chunk = link.transfer_chunk(replica, "WEBDAV", n_threads=n_threads, job_id=self.id, read=read)
                read += chunk
                yield self.env.timeout(Time.SECOND)

            #delet this
            end = self.env.now
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
            ConTh -= replica.size
            grid.transfer_datasets[key].append([S, ConTh, ConPr, T])
            #end delet this

            self.remote_data_access_threads -= 1
            if self.remote_data_access_threads == 0:
                link.campaign_load -= 1
        else:
            raise Exception('Invalid data access behavior.')

    def run(self):
        read_requests = []
        for i in range(len(self.replicas)):
            read_requests.append(self.env.process(self.read_data(i)))
        yield from read_requests
        # process replicas based on mips?
