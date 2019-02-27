"""
This class contains meta-information on data transfer protocols.
"""



class Protocol:

    def __init__(self, overhead):
        self.overhead = overhead
        self.description = None
