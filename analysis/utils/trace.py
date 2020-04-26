import string
import random

from .db import session, Token, TraceModel
from time import time

class TraceFactory:

    @staticmethod
    def build(json_trace):

        return TraceModel(
            id=json_trace['id'],
            outdoor=json_trace['outdoor'],
            activity=json_trace['activity'],
            public_transportation=json_trace['public_transportation']
        )
