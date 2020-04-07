import requests
from datetime import datetime
import json
from multiprocessing import Pool
from analysis import BACKUP_DOCUMENTS_PATH
from analysis import READ_API_URL
from analysis import READ_TOKEN
from analysis.utils.db import session
from analysis.utils.factory import IndividualReportFactory
from sqlalchemy.exc import IntegrityError
from json.decoder import JSONDecodeError

DATE_PARAM_FORMAT = '%Y-%m-%dT%H:%M:%S'
DATE_HOUR_PARAM_FORMAT = '%Y-%m-%dT%H'


def download_json(from_unix: int, to_unix: int):

    start_time = datetime.fromtimestamp(from_unix)
    start_time_str = start_time.strftime(DATE_PARAM_FORMAT)

    end_time = datetime.fromtimestamp(to_unix)
    end_time_str = end_time.strftime(DATE_PARAM_FORMAT)

    payload = {'start': start_time_str, 'end': end_time_str, 'token': READ_TOKEN}
    try:
        resp = requests.post(READ_API_URL, data=payload)
        data = resp.json()
        if 'error' in data:
            # Empty collection
            # print(data['error'])
            return []
        else:
            return data
    except JSONDecodeError:
        print('JSONDecodeError: ',resp.url)
    except UnboundLocalError as e:
        print('UnboundLocalError: ',str(e))
    return []


def download_data(from_time, to_time, period_minutes):

    # clean the current raw-data
    for _f in BACKUP_DOCUMENTS_PATH.glob('*'):
        _f.unlink()

    _from = int(datetime.strptime(from_time, DATE_PARAM_FORMAT).timestamp())
    _to = int(datetime.strptime(to_time, DATE_PARAM_FORMAT).timestamp())

    _previous = _from
    _next = _previous + 60 * period_minutes
    while _next <= _to:

        print("Download from " + datetime.fromtimestamp(_previous).strftime(DATE_PARAM_FORMAT) + " to " + datetime.fromtimestamp(_next).strftime(DATE_PARAM_FORMAT))
        try:
            download_json(_previous, _next)
        except Exception as e:
            print(e)
        _previous = _next
        _next = _previous + (60 * period_minutes)


def run_one_shot():
    from_time = '2020-03-22T15:00:00'
    to_time = '2020-03-22T17:00:00'
    period_minutes = 15  # correspond to 5500 object if 8Mio of person are reporting each day
    download_data(from_time, to_time, period_minutes)


def download_worker(args):
    _previous_minute = args[0]
    _next_minute = args[1]
    data = download_json(_previous_minute,_next_minute)
    _prev_minute_str = datetime.fromtimestamp(_previous_minute).strftime(DATE_PARAM_FORMAT)
    _next_minute_str = datetime.fromtimestamp(_next_minute).strftime(DATE_PARAM_FORMAT)

    print(" * downloaded for " + str(_prev_minute_str) + " with " + str(len(data)) + " elements" )
    return data


def download_hours_frame(_from_hour: str, _to_hour: str, worker_frame_minute: int = 10):
    if 60 % worker_frame_minute != 0:
        raise Exception("wrong worker period")

    _from = int(datetime.strptime(_from_hour, DATE_HOUR_PARAM_FORMAT).timestamp())
    _to = int(datetime.strptime(_to_hour, DATE_HOUR_PARAM_FORMAT).timestamp())

    existing_files = [
        file.name for file in BACKUP_DOCUMENTS_PATH.glob('*/*.json')
    ]
    now = datetime.now()  # current date and time
    now_str = now.strftime("%Y-%m-%d-%H-%M-%S")
    backup_dir = BACKUP_DOCUMENTS_PATH / ('download-time-' + now_str)
    if not backup_dir.exists():
        backup_dir.mkdir()

    # iterate over hours
    for _previous in range(_from, _to, 60 * 60):
        _next = _previous + 60 * 60
        _prev_str = datetime.fromtimestamp(_previous).strftime(DATE_HOUR_PARAM_FORMAT)
        _next_str = datetime.fromtimestamp(_next).strftime(DATE_HOUR_PARAM_FORMAT)
        filename = ('data-' + _prev_str + '_' + _next_str + '.json')
        if filename in existing_files:
            print("Skipping", filename)
            continue
        else:
            print("Download from", _prev_str, "to", _next_str)

        minutes = list()

        # iterate over minutes
        _previous_minute = _previous
        _to_minute = _next
        _next_minute = _previous_minute + 60 * worker_frame_minute
        while _next_minute <= _to_minute:
            minutes.append((_previous_minute, _next_minute))
            _previous_minute = _next_minute
            _next_minute = _previous_minute + worker_frame_minute * 60

        hour_data = []
        # launch pool
        process_count = int(60 / worker_frame_minute)
        with Pool(processes=process_count) as pool:
            res = pool.map(download_worker, minutes)
            for worker_results in res:
                if len(worker_results) > 0:
                    for data in worker_results:
                        hour_data.append(data)

        # save results to json file
        if len(hour_data) > 0:
            file_path = backup_dir / filename
            with open(str(file_path), 'w') as outfile:
                json.dump(hour_data, outfile)

        try:
            # try batch commit
            for report_json in hour_data:
                report = IndividualReportFactory.build(report_json)
                session.add(report)
            session.commit()
        except IntegrityError:
            session.rollback()
            # duplication, then one by one
            print('Warn: Duplicate in batch commit')
            for report_json in hour_data:
                try:
                    report = IndividualReportFactory.build(report_json)
                    session.add(report)
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    print('Warn: Duplicate doc id: ' + report_json['id'])

