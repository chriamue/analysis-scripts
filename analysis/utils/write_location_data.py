import pandas as pd
from analysis.utils.db import session, LocationModel


def load():
    q = session.query(LocationModel)
    locations = q.all()
    all_locations = (loc.serialize() for loc in locations)

    df = pd.DataFrame(all_locations)
    return df


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        path = "merge-all-days.csv"

    df = load()
    df.to_csv(path, index=False)