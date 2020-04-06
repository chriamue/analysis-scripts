"""
Created by nuffer at 3/23/20

"""
# from analysis.utils.analysis import analysis_next_report
from analysis.utils.analysis_symptom import (
    map_calculate,
    group_reports_by_location,
    fix_nan,
)
from analysis.utils.analysis import count_report_to_analyse


def run_analysis_for_all(batch_size=1000):

    while count_report_to_analyse() > 0:
        # analysis_next_report(batch_size)
        map_calculate(batch_size)

    group_reports_by_location()
    fix_nan()


if __name__ == "__main__":

    run_analysis_for_all(1000)
