import csv
from analysis import TMP_PATH, GEOCODING_RAW_FILE_URL
from analysis.utils.db import LocationModel
from analysis.utils.db import session
import requests


GEOCODING_RAW_FILE = TMP_PATH / 'geocoding.csv'


def download_geocoding_file():
    print("downloading geocoding file...")
    response = requests.get(GEOCODING_RAW_FILE_URL)
    data = response.text
    f = open(str(GEOCODING_RAW_FILE), "w")
    f.write(data)
    f.close()
    print("downloading geocoding file: done")


def upload_geo_data():
    """
     csv headers: country_code,postal_code,latitude,longitude,region_id
    :return:
    """

    geo_locations = {}
    with open(str(GEOCODING_RAW_FILE), 'r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i == 0:
                continue  # skip header

            location = {
                'country_code': row[1],
                'postal_code': row[2],
                'longitude': float(row[3]),
                'latitude': float(row[4]),
                'region_id': row[5],
            }

            # remove duplicate by this...
            geo_locations[location['postal_code']] = location

    for key, loc in geo_locations.items():
        location = LocationModel(
            postal_code=loc['postal_code'],
            country_code=loc['country_code'],
            region_id=loc['region_id'],
            longitude=loc['longitude'],
            latitude=loc['latitude'],
        )
        session.add(location)
    session.commit()