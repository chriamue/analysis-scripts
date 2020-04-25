import string
import random

from .db import session, Token


def generate_token(length=16):
    allowed_characters = string.ascii_letters + string.digits
    token = ''.join(random.choice(allowed_characters) for i in range(length))

    q = session.query(Token).filter_by(token=token).first()

    if q:
        generate_token()
    else:
        return token


class ReportFactory:

    @staticmethod
    def build(parameter_list):
        pass
