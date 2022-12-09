# 2022-12-08 @herpichfr
# Herpich, Fabio R - fabiorafaelh@gmail.com
#

import pandas as pd
from precovery.orbit import Orbit, EpochTimescale
from precovery.main import precover

def read_mpc_database(mpcddbpath):
    """Read and format MPC database to a table format"""
    df = pd.read_json("mpcorb_extended.json.gz")
    # select only more recent observations to avoid crashing pyorb
    mask = df['Last_obs'] > '2015-01-01'
    df2 = df[["Number", "Name", "a", "e", "i", "Node", "Peri", "M", "Epoch", "H", "G", "Last_obs"]][mask]

    return df2

def search_orbit_inDB(mpcdf, indexes):
    """Search MPC orbit within precovery DB"""
    for index in indexes:
        if index == -99:
            print('fake name. Skipping')
        else:
            df = mpcdf.iloc[index]
            orbit = Orbit.keplerian(
                0,
                df['a'], df['e'], df['i'], df['Node'], df['Peri'], df['M'],
                df['Epoch'] - 2400000.5,
                EpochTimescale.TT,
                20,
                0.15)

            results = precover(orbit, DB_DIR, tolerance=1/3600)
            print(results)
            # need to save dataframe


    return

    # Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # get MPC database
    mpcdbpath = '/epyc/ssd/users/herpich2/mpcorb_extended.json.gz'
    mpcdf = read_mpc_database(mpcdbpath)

    DB_DIR = '/epyc/ssd/users/herpich2/splus_idr4_nomatch_new/'
    for ind in [[209], [210]]:
        search_orbit_inDB(mpcdf, ind)

    # print(result)
    # end here