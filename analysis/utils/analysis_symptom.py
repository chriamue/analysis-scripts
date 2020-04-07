import time
from datetime import datetime, date

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import InternalError

from analysis.utils.db import engine, session
from analysis.utils.db import DailyDiagnosticChangeModel
from analysis.utils.db import IndividualReportModel
from analysis.utils import db_enum as enum


COLUMNS = (
    "healthy",
    "sick_guess_no_corona",
    "sick_guess_corona",
    "sick_corona_confirmed",
    "recovered_confirmed",
    "recovered_not_confirmed",
)


def calculate(report):
    """Calculate symptom factor per report / row."""
    S = sum(
        column.value
        for column in (
            enum.Scale3[report.temp],
            enum.Scale4[report.cough],
            enum.Scale4[report.breathless],
            enum.Energy[report.energy],
        )
    )
    return S


def map_calculate(collection_size: int):
    """Calcalate symptom factor S for the whole DB where analysis_done = 0"""
    start_time_analysis = time.time()

    with engine.begin() as con:
        # load the next collection of reports to analyse
        next_reports = pd.read_sql_query(
            (
                "SELECT * FROM individual_report WHERE analysis_done = 0 "
                "ORDER BY timestamp LIMIT "
            )
            + str(collection_size),
            con=con,
            index_col="document_id",
        )

        S = next_reports.apply(calculate, axis=1)

        query = """
            UPDATE individual_report
            SET S = (SELECT 0 from temp_table),
                analysis_done = 1
            WHERE (
                SELECT document_id from temp_table d
                WHERE individual_report.document_id = document_id
            )
        """
        S.to_sql(
            "temp_table",
            con,
            if_exists="replace",
            dtype={"document_id": sa.String(30), "S": sa.Integer,},
        )
        try:
            con.execute(query)
        except Exception as e:
            print(e)
            print("ERROR: while executing query: {}".format(query))
        try:
            con.execute("DROP TABLE temp_table")
        except InternalError:
            print("WARNING: no temp_table to drop")

    session.commit()
    spend_time = time.time() - start_time_analysis
    print("Analysed {} samples in {} s".format(collection_size, spend_time))


def group_reports_by_location():
    start_time_analysis = time.time()

    # load all analysed reports
    df = pd.read_sql(
        "SELECT * FROM individual_report WHERE analysis_done = 1",
        con=engine,
        index_col="document_id",
    )

    # diagnosis using S factors
    # healthy = df.query('S == 0')
    # sick_guess_no_corona = df.query('0 < S < 4')
    # sick_guess_corona = df.query('S >= 4')

    # diagnosis from report
    # healthy = df.query('diagnostic == 0')
    # sick_guess_no_corona = df.query('diagnostic == 1')
    # sick_guess_corona = df.query('diagnostic == 2')
    # sick_corona_confirmed = df.query('diagnostic == 3')
    # recovered_confirmed = df.query('diagnostic == 4')
    # recovered_not_confirmed = df.query('diagnostic == 5')

    def group(df_diagnosis):
        """Get number of reports by location"""
        return df_diagnosis.groupby("locator").sum()

    def to_sql(totals, column):
        query = """
            UPDATE locations AS old, temp_totals AS new
            SET old.{column} = new.analysis_done
            WHERE old.postal_code = new.locator
        """.format(
            column=column
        )
        with engine.begin() as con:
            totals.to_sql(
                "temp_totals",
                con,
                if_exists="replace",
                dtype={"locator": sa.Integer,},
            )
            try:
                con.execute(query)
            except:
                print("ERROR: while executing query: {}".format(query))
            try:
                con.execute("DROP TABLE temp_totals")
            except InternalError:
                print("WARNING: no temp_totals to drop")

    # total_healthy = group(healthy)
    # print(total_healthy)
    # to_sql(total_healthy, "total_healthy")
    for diagnostic, column in enumerate(COLUMNS):
        df_diagnosis = df.query("diagnostic == {}".format(diagnostic))
        df_totals = group(df_diagnosis)
        to_sql(df_totals, "total_{}".format(column))

    session.commit()
    spend_time = time.time() - start_time_analysis
    print("Grouped {} samples by location in {} s".format(len(df), spend_time))


def fix_nan():
    """Remove if all diagnostic columns are NaN and fill remaining nan with
    zeros.

    """
    df = pd.read_sql(
        "SELECT * FROM locations", con=engine, index_col="postal_code",
    )
    # Keep only the rows with at least 5 non-NA values.
    nb_columns = len(COLUMNS)
    df.dropna(thresh=nb_columns - 1, inplace=True)
    df.fillna(0., inplace=True)
    # Alternatively:
    # df.dropna(
    #    how="all",
    #    subset=["total_" + column for column in COLUMNS],
    #    inplace=True
    # )
    #  print(df)
    with engine.begin() as con:
        df.to_sql(
            "locations",
            con,
            if_exists="replace",
            dtype={"locator": sa.Integer},
        )


if __name__ == "__main__":
    map_calculate(10)
    group_reports_by_location()
    fix_nan()
