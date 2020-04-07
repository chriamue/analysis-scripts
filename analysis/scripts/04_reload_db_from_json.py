"""
Created by nuffer at 3/25/20

"""
import json
from analysis.utils.factory import IndividualReportFactory
from analysis.utils.db import session
from analysis import BACKUP_DOCUMENTS_PATH


if __name__ == '__main__':

    for file in BACKUP_DOCUMENTS_PATH.glob('*/*.json'):
        print('Loading file: ' + str(file))
        with open(str(file)) as json_file:
            data = json.load(json_file)
            for report in data:
                report = IndividualReportFactory.build(report)
                session.add(report)
            session.commit()

