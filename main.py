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

def search_orbit_inDB(mpcdf):
    """Search MPC orbit within precovery DB"""
    orbit = Orbit.keplerian(
        0,
        mpcdf['a'], mpcdf['e'], mpcdf['i'], mpcdf['Node'], mpcdf['Peri'], mpcdf['M'],
        mpcdf['Epoch'] - 2400000.5,
        EpochTimescale.TT,
        20,
        0.15)

    results = precover(orbit, DB_DIR, tolerance=1/3600)

    return results

    # Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # get MPC database
    mpcdbpath = '/ssd/splus/asteroids/mpc-database/mpcorb_extended.json.gz'
    mpcdf = read_mpc_database(mpcdbpath)

    DB_DIR = '/epyc/ssd/users/herpich2/splus_idr4_nomatch_new/'
    for i in range(mpcdf['Name'].size)[:1]:
        result = search_orbit_inDB(mpcdf.iloc[i])

    # end here