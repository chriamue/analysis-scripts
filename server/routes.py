from server import app
from server.invalid_usage import InvalidUsage
from flask import jsonify, request, abort, make_response
from sqlalchemy.exc import IntegrityError

from analysis.utils.factory import IndividualReportFactory
from analysis.utils.db import session, init_db
from analysis.utils.db import LocationModel, IndividualReportModel, Token
from analysis.utils.geo import download_geocoding_file, upload_geo_data
from analysis.utils.analysis_symptom import (
    map_calculate,
    group_reports_by_location,
)
from analysis.utils.analysis import count_report_to_analyse

from analysis.utils.report import generate_token


import sys


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/report', methods=['POST'])
def report():
    data = request.json
    print(data)

    # if token submitted
        # if token valid
            # insert report into database
    # else
        # generate new token
        # insert report into database
        # return token

    if 'token' in data.keys():
        submitted_token = str(data['token'])
        if len(submitted_token) == 16:
            pass
        else:
            raise InvalidUsage("Invalid token")
        
        q = session.query(Token).filter_by(token=submitted_token).first()
        if q:
            # insert report into database
            pass
        else:
            raise InvalidUsage("Provided token doesn't exist")
    elif 'report' in data.keys():
        generated_token = generate_token()
        data['token'] = generated_token
        print(data)
        
        # insert report into database
        pass
    else:
        raise InvalidUsage("Required parameters are missing")
        

@app.route('/init', methods=['GET'])
def init():
    """Initialize database"""
    init_db()
    download_geocoding_file()
    try:
        upload_geo_data()
    except IntegrityError:
        session.rollback()
        abort(400, "Database already initialized!")
    return "Initialized DB and uploaded location data"

@app.route('/locations', methods=['GET'])
def all_locations():
    q = session.query(LocationModel)
    locations = q.all()
    return jsonify(locations= [loc.serialize() for loc in locations])

@app.route('/reports', methods=['GET'])
def all():
    q = session.query(IndividualReportModel)
    individuals = q.all()
    return jsonify(individials= [individual.serialize() for individual in individuals])

@app.route('/analyse', methods=['GET'])
def analyse():
    """Run analysis over reports of batch size `size`.
    Examples
    --------

    /analyze?size=1000

    """
    batch_size = request.args.get('size', default=1000, type=int)
    nb_count = count_report_to_analyse()
    if nb_count:
        while count_report_to_analyse() > 0:
            map_calculate(batch_size)

        group_reports_by_location()
        return "Computed upto {} reports".format(batch_size)
    else:
        abort(400, "No reports left to analyse!")


@app.route('/<int:id>', methods=['GET'])
def get(id):
    return jsonify([id])

@app.route('/add-person', methods=['POST'])
def add_person():
    if not request.json:
        raise InvalidUsage("No data supplied")
    
    param_list = ['id', 'data']
    check_param(request.json, param_list)

    data_list = ['diagnostic', 'locator', 'sessionId', 'timestamp']
    check_param(request.json['data'], data_list)
    
    timestamp_list = ["_seconds", "_nanoseconds"]
    check_param(request.json['data']['timestamp'], timestamp_list)


    try:
        report = IndividualReportFactory.build(request.json)
        try:
            session.add(report)
            session.commit()
        except:
            session.rollback()
            abort(500,"Could not insert to database")
    except TypeError:
        raise InvalidUsage("Some parameter was wrongly typed (string, int, array).")
    except:
        message = ("Could not create Individual Report. Probably malformed json. JSON:{%s}, %s", request.json)
        abort(400, message)


def check_param(obj, param_list):
    for param in param_list:
        if (not param in obj) or (obj[param] == None):
            raise InvalidUsage("parameter '%s' is not supplied" % param)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response