"""
Abstract Host class.
"""

import uuid


class Host:

    def __init__(self):
        self.id = uuid.uuid4().int
