import string
import random

from .db import session, Token, IndividualReportModel, Comorbid
from time import time


def generate_token(length=16):
    allowed_characters = string.ascii_letters + string.digits
    token = ''.join(random.choice(allowed_characters) for i in range(length))

    q = session.query(Token).filter_by(token=token).first()

    if q:
        generate_token()
    else:
        return token


class TokenFactory:

    @staticmethod
    def build(token):
        return Token(token=token, timestamp=int(time()))


class ReportFactory:

    @staticmethod
    def build(json_report):

        return IndividualReportModel(
            token_id=json_report['token'],
            locator=json_report['report']['postal_code'],
            timestamp=int(time()),
            temp=json_report['report']['temp'],
            cough=json_report['report']['cough'],
            breathless=json_report['report']['breathless'],
            energy=json_report['report']['energy'],
            exposure=json_report['report']['exposure'],
            has_comorbid=json_report['report']['has_comorbid'],
            compromised_immune=json_report['report']['compromised_immune'],
            age=json_report['report']['age'],
        )


class ComorbidFactory:

    @staticmethod
    def build(comorbid_json):

        return Comorbid(
            parent_id=comorbid_json['parent_id'],
            hypertension=comorbid_json['hypertension'],
            cardiovascular=comorbid_json['cardiovascular'],
            pulmonary=comorbid_json['pulmonary'],
            cancer=comorbid_json['cancer'],
            diabetes=comorbid_json['diabetes'],
            renal=comorbid_json['renal'],
            neurological=comorbid_json['neurological'],
            respiratory=comorbid_json['respiratory']
        )
