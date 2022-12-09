# 2022-12-08 @herpichfr
# Herpich, Fabio R - fabiorafaelh@gmail.com
#
import os.path
import numpy as np
import pandas as pd
from precovery.orbit import Orbit, EpochTimescale
from precovery.main import precover
import multiprocessing
import time

def read_mpc_database(mpcddbpath):
    """Read and format MPC database to a table format"""
    df = pd.read_json("mpcorb_extended.json.gz")
    # select only more recent observations to avoid crashing pyorb
    mask = df['Last_obs'] > '2015-01-01'
    df2 = df[["Number", "Name", "a", "e", "i", "Node", "Peri", "M", "Epoch", "H", "G", "Last_obs"]][mask]

    return df2

def search_orbit_inDB(mpcdf, indexes):
    """Search MPC orbit within precovery DB"""
    DB_DIR = '/epyc/ssd/users/herpich2/splus_idr4_nomatch_new/'
    for index in indexes:
        if index == -99:
            print('fake name. Skipping')
        else:
            print('calculating orbits for index', index)
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
            if len(results) > 0:
                savedirpath = '/astro/store/epyc3/projects3/splus_dataset/found_asteroids_20221208/'
                if not os.path.isdir(savedirpath):
                    os.mkdir(savedirpath)
                print('saving results for index', index, 'to', savedirpath)
                pd.to_csv(savedirpath + 'mp_cast' + repr(1000000 + index) + '.csv', index=False)

    return

    # Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # get MPC database
    mpcdbpath = '/epyc/ssd/users/herpich2/mpcorb_extended.json.gz'
    mpcdf = read_mpc_database(mpcdbpath)

    num_procs = 4
    l = list(mpcdf.index)[:11]
    numrows = len(l)
    if numrows % num_procs > 0:
        print('increasing number of indexes')
        increase_to = int(numrows / num_procs) + 1
        i = 0
        while i < (increase_to * num_procs - numrows):
            l.append(int(-99))
            i += 1
        else:
            print('number of lines already satisfy the conditions for num_proc')
    indexes = np.array(l).reshape((num_procs, int(np.array(l).size / num_procs)))

    print('calculating for a total of', indexes.size, 'rows')
    # create jobs
    jobs = []
    print('creating', num_procs, 'jobs...')

    for ind in indexes:
        process = multiprocessing.Process(target=search_orbit_inDB, args=(mpcdf, ind))
        jobs.append(process)

    # start jobs
    print('starting', num_procs, 'jobs!')
    for j in jobs:
        j.start()

    # check if any of the jobs initialized previously still alive
    # save resulting table after all are finished
    proc_alive = True
    while proc_alive:
        if any(proces.is_alive() for proces in jobs):
            proc_alive = True
            time.sleep(1)
        else:
            print('All jobs finished')
            proc_alive = False

    print('Done!')