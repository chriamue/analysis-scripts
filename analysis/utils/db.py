from datetime import date
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from analysis import DATABASE, MYSQL_PORT, DATABASE_URL, DATABASE_USERNAME, DATABASE_PASSWORD
from analysis.utils import db_enum as enum


base = declarative_base()
db_string = "mysql+pymysql://" + DATABASE_USERNAME + ":" + DATABASE_PASSWORD + "@" + DATABASE_URL + ":" + MYSQL_PORT + '/' + DATABASE
print("Connecting to db: %s" % db_string)
engine = sa.create_engine(db_string)
base.metadata.bind = engine
session = orm.scoped_session(orm.sessionmaker())(bind=engine)


class Token(base):
    __tablename__ = 'token'

    token = sa.Column(sa.String(16), primary_key=True)
    timestamp = sa.Column(sa.BigInteger())


class IndividualReportModel(base):
    """JSON data is translated into SQL table columns."""
    __tablename__ = 'individual_report'
    document_id = sa.Column(sa.Integer(), primary_key=True, autoincrement=True)

    token_id = sa.Column(sa.String(16), sa.ForeignKey("token.token"))
    token = orm.relationship("Token", foreign_keys=[token_id])

    diagnostic = sa.Column(sa.Integer,nullable=True)
    locator = sa.Column(sa.String(15),nullable=False)
    session_id = sa.Column(sa.String(50),nullable=True)
    timestamp = sa.Column(sa.BigInteger, nullable=False)
    symptoms = sa.Column(sa.String(255))
    analysis_done = sa.Column(sa.Boolean,nullable=False, default=False)

    # **************
    # Key symptoms
    # ------------
    temp = sa.Column(sa.Enum(enum.Scale3), nullable=False)
    cough = sa.Column(sa.Enum(enum.Scale4), nullable=False)
    breathless = sa.Column(sa.Enum(enum.Scale4), nullable=False)
    energy = sa.Column(sa.Enum(enum.Energy), nullable=False)

    exposure = sa.Column(sa.Enum(enum.Exposure))

    has_comorbid = sa.Column(sa.Boolean)
    comorbid = orm.relationship(
        "Comorbid", uselist=False, back_populates="parent"
    )

    compromised_immune = sa.Column(sa.Boolean)
    age = sa.Column(sa.Enum(enum.Scale3))

    # final symptom risk factor
    S = sa.Column(sa.Integer, default=0)

    def __repr__(self):
        return '<Indiv. report: NPA ' + self.locator + ' time ' + str(self.timestamp) + '>'

    def serialize(self):
        """The response by the REST-API"""
        return {
            'locator': self.locator,
            'analysis_done': self.analysis_done,
            'S': self.S,
        }



class Comorbid(base):
    """Do you have any of the following ongoing illnesses? (multiple choice)

    One-to-one mapping
    ref: https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html#one-to-one
    """
    __tablename__ = "comorbidities"
    parent_id = sa.Column(
        sa.Integer(),
        sa.ForeignKey('individual_report.document_id'),
        primary_key=True,
    )
    parent = orm.relationship(
        "IndividualReportModel", back_populates="comorbid"
    )
    hypertension = sa.Column(sa.Boolean)
    cardiovascular = sa.Column(sa.Boolean)
    pulmonary = sa.Column(sa.Boolean)
    cancer = sa.Column(sa.Boolean)
    diabetes = sa.Column(sa.Boolean)
    renal = sa.Column(sa.Boolean)
    neurological = sa.Column(sa.Boolean)
    respiratory = sa.Column(sa.Boolean)


class DailyDiagnosticChangeModel(base):
    __tablename__ = 'daily_diagnostic_change'
    id = sa.Column(sa.Integer, primary_key=True,autoincrement=True)
    locator = sa.Column(sa.String(15))
    date = sa.Column(sa.Date)
    diagnostic_0 = sa.Column(sa.Integer, default=0)
    diagnostic_1 = sa.Column(sa.Integer, default=0)
    diagnostic_2 = sa.Column(sa.Integer, default=0)
    diagnostic_3 = sa.Column(sa.Integer, default=0)
    diagnostic_4 = sa.Column(sa.Integer, default=0)
    diagnostic_5 = sa.Column(sa.Integer, default=0)

    def __repr__(self):
        d = str(self.year) + '-' + str(self.month) + '-' + str(self.day)
        change = [
            self.diagnostic_0,
            self.diagnostic_1,
            self.diagnostic_2,
            self.diagnostic_3,
            self.diagnostic_4,
        ]
        return '<DailyChang: NPA ' + self.locator + ' ' + d + ' ' + str(change) + '>'


class LocationModel(base):
    """
    headers from csv: country_code,postal_code,town,region,latitude,longitude
    """
    __tablename__ = 'locations'
    postal_code = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    country_code = sa.Column(sa.String(3))
    region_id = sa.Column(sa.Text)
    longitude = sa.Column(sa.Float)
    latitude = sa.Column(sa.Float)

    # Computed
    date = sa.Column(sa.Date())
    total_healthy = sa.Column(sa.Integer)
    total_sick_guess_no_corona = sa.Column(sa.Integer)
    total_sick_guess_corona = sa.Column(sa.Integer)
    total_sick_corona_confirmed = sa.Column(sa.Integer)
    total_recovered_confirmed = sa.Column(sa.Integer)
    total_recovered_not_confirmed = sa.Column(sa.Integer)

    def __repr__(self):
        return '<Location: ' + self.postal_code + '>'

    def serialize(self):
        """The response by the REST-API"""
        return {
            'date': self.date,
            'state': self.region_id.split("::")[0],
            'npa_plz' : self.postal_code,
            'longitude' : self.longitude,
            'latitude' : self.latitude,
            'total_healthy' : self.total_healthy,
            'total_sick_guess_no_corona' : self.total_sick_guess_no_corona,
            'total_sick_guess_corona' : self.total_sick_guess_corona,
            'total_sick_corona_confirmed' : self.total_sick_corona_confirmed,
            'total_recovered_confirmed' : self.total_recovered_confirmed,
            'total_recovered_not_confirmed' : self.total_recovered_not_confirmed,
        }

def init_db():
    base.metadata.create_all()
